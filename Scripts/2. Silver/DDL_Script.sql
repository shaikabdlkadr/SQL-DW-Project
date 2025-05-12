/*
=================================================================================================================================
DDL Script: Create Silver Tables
=================================================================================================================================

Script Purpose:
    - This script creates tables in the 'silver' schema, dropping existing tables. if they already exist.
	- Run this script to re-define the DDL structure of 'silver' Tables.
    - Added a new column dwh_created_date column (metadata column) to know when the data got loaded into the silver layer,
     - easy to know when the data was last updated.
=================================================================================================================================
*/

DROP TABLE IF EXISTS silver.crm_cust_info;

CREATE TABLE IF NOT EXISTS silver.crm_cust_info(
    cust_id INTEGER,
    cst_key TEXT,
    cst_firstname TEXT,
    cst_lastname TEXT,
    cst_marital_status TEXT,
    cst_gndr TEXT,
    cst_create_date DATE,
    dwh_created_date TIMESTAMP DEFAULT NOW()
);


DROP TABLE IF EXISTS silver.crm_prd_info;

CREATE TABLE IF NOT EXISTS silver.crm_prd_info(
    prd_id       INTEGER,
    prd_cat      TEXT,
    prd_key_sls  TEXT,
    prd_name     TEXT,
    prd_cost     INTEGER,
    prd_line     TEXT,
    prd_start_dt DATE,
    prd_end_dt   DATE,
    dwh_created_date TIMESTAMP DEFAULT NOW()
);


DROP TABLE IF EXISTS silver.crm_sales_details;

CREATE TABLE IF NOT EXISTS silver.crm_sales_details(
    sls_ord_num  TEXT,
    sls_prd_key  TEXT,
    sls_cust_id  INTEGER,
    sls_order_dt DATE,
    sls_ship_dt  DATE,
    sls_due_dt   DATE,
    sls_sales    INTEGER,
    sls_quantity INTEGER,
    sls_price    INTEGER,
    dwh_created_date TIMESTAMP DEFAULT NOW()
);


DROP TABLE IF EXISTS silver.erp_cust_az12;

CREATE TABLE IF NOT EXISTS silver.erp_cust_az12(
    cid    TEXT,
    bdate  DATE,
    gen    TEXT,
    dwh_created_date TIMESTAMP DEFAULT NOW()
);


DROP TABLE IF EXISTS silver.erp_loc_a101;

CREATE TABLE IF NOT EXISTS silver.erp_loc_a101(
    cid    TEXT,
    cntry  TEXT,
    dwh_created_date TIMESTAMP DEFAULT NOW()
);


DROP TABLE IF EXISTS silver.erp_px_cat_g1v2;

CREATE TABLE IF NOT EXISTS silver.erp_px_cat_g1v2(
    id           TEXT,
    cat          TEXT,
    subcat       TEXT,
    maintenance  TEXT,
    dwh_created_date TIMESTAMP DEFAULT NOW()
);
