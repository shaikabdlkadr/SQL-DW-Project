/*
===============================================================================
DDL Script: Create Gold Views
===============================================================================

Script Purpose:
    - This script creates views for the Gold layer in the data warehouse. 
    - The Gold layer represents the final dimension and fact tables (Star Schema).
    - Each view performs transformations and combines data from the Silver layer to 
    produce a clean, enriched, and business-ready dataset.

Usage:
    - These views can be queried directly for analytics and reporting.
===============================================================================
*/

-- =============================================================================
-- Create Dimension: gold.dim_customers
-- =============================================================================

DROP VIEW IF EXISTS gold.dim_customers;

CREATE OR REPLACE VIEW gold.dim_customers AS
    SELECT ROW_NUMBER() OVER(ORDER BY ci.cust_id) AS customer_key, ci.cust_id AS customer_id, ci.cst_key AS customer_number,
        ci.cst_firstname AS first_name, ci.cst_lastname AS last_name, cl.cntry AS country, ci.cst_marital_status AS marital_status,
        CASE WHEN ci.cst_gndr <> 'n/a' THEN ci.cst_gndr  -- CRM is the master for Customer details and info
        ELSE COALESCE(ca.gen, 'n/a') END AS gender, 
         ca.bdate AS birth_date, ci.cst_create_date AS create_date
    FROM silver.crm_cust_info ci
    LEFT JOIN silver.erp_cust_az12 ca
        ON ci.cst_key = ca.cid
    LEFT JOIN silver.erp_loc_a101 cl
        ON ci.cst_key = cl.cid;

-- =============================================================================
-- Create Dimension: gold.dim_products
-- =============================================================================

DROP VIEW IF EXISTS gold.dim_products;

CREATE OR REPLACE VIEW gold.dim_products AS
    SELECT ROW_NUMBER() OVER (ORDER BY c.prd_start_dt, c.prd_key) AS product_key, 
        c.prd_id AS Product_ID, c.prd_key AS Product_Number, c.prd_name AS Product_Name, 
        c.prd_cat AS Category_ID, e.cat AS Category, e.subcat AS Subcategory,
        e.maintenance, c.prd_cost AS Cost, c.prd_line AS Product_Line, c.prd_start_dt AS Start_Date
        -- c.prd_end_dt 
    FROM silver.crm_prd_info c
    LEFT JOIN silver.erp_px_cat_g1v2 e
        ON c.prd_cat = e.id
    WHERE c.prd_end_dt IS NULL;		-- Filtering out to current products


-- =============================================================================
-- Create Fact: gold.fact_sales
-- =============================================================================

DROP VIEW IF EXISTS gold.fact_sales;

CREATE OR REPLACE VIEW gold.fact_sales AS 
    SELECT sls_ord_num AS order_number, p.product_key, c.customer_key,
        sls_order_dt AS order_date, sls_ship_dt AS shipping_date, sls_due_dt AS due_date,
        sls_sales AS sales_amount, sls_quantity AS quantity, sls_price AS price
    FROM silver.crm_sales_details s
    LEFT JOIN gold.dim_products p
        ON s.sls_prd_key = p.product_number
    LEFT JOIN gold.dim_customers c
        ON s.sls_cust_id = c.customer_id;