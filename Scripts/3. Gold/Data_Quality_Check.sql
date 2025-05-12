/*
===============================================================================
Quality Checks
===============================================================================
Script Purpose:
    This script performs quality checks to validate the integrity, consistency, 
    and accuracy of the Gold Layer. These checks ensure:
    - Uniqueness of surrogate keys in dimension tables.
    - Referential integrity between fact and dimension tables.
    - Validation of relationships in the data model for analytical purposes.

Usage Notes:
    - Investigate and resolve any discrepancies found during the checks.
===============================================================================
*/

-- gold.dim_customers
-- Expectation: No results 

-- Finding any duplicates after joining the CRM, ERP customer info tables

SELECT cst_key, COUNT(*) FROM (
	SELECT ci.cust_id, ci.cst_key, ci.cst_firstname, ci.cst_lastname, ci.cst_marital_status, ci.cst_gndr, 
		ca.bdate, cl.cntry, ci.cst_create_date
	FROM silver.crm_cust_info ci
	LEFT JOIN silver.erp_cust_az12 ca
		ON ci.cst_key = ca.cid
	LEFT JOIN silver.erp_loc_a101 cl
		ON ci.cst_key = cl.cid
) subQ
GROUP BY 1
HAVING COUNT(*) <> 1;

-- Gender validation and final updation

SELECT DISTINCT ci.cst_gndr, ca.gen, CASE
	WHEN ci.cst_gndr <> 'n/a' THEN ci.cst_gndr
	ELSE COALESCE(ca.gen, 'n/a') END AS gndr_upd
FROM silver.crm_cust_info ci
LEFT JOIN silver.erp_cust_az12 ca
	ON ci.cst_key = ca.cid;

-- gold.dim_products
-- Expectation: No results 
-- Foreign Key integrity check

SELECT product_key, COUNT(*) AS duplicate_count
FROM gold.dim_products
GROUP BY product_key
HAVING COUNT(*) > 1;

-- gold.fact_sales
-- Check the data model connectivity between fact and dimensions

SELECT *
FROM gold.fact_sales s
LEFT JOIN gold.dim_customers c
	ON s.customer_key = c.customer_key
LEFT JOIN gold.dim_products p
	ON s.product_key = p.product_key
WHERE s.product_key IS NULL OR c.customer_key IS NULL