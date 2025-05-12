-- creation of Schemas and Database created;

/*
========================================================================================================================================
DDL Script: Create Bronze Tables
========================================================================================================================================

Script Purpose:
    - This script creates tables in the 'bronze' schema, dropping existing tables 
    - if they already exist.
	- Run this script to re-define the DDL structure of 'bronze' Tables

Alert:
    - While creating the table concetrate on the table naming convensions and column names and data types.
========================================================================================================================================
*/

DROP TABLE IF EXISTS bronze.crm_cust_info;

CREATE TABLE IF NOT EXISTS bronze.crm_cust_info(
    cust_id INTEGER,
    cst_key TEXT,
    cst_firstname TEXT,
    cst_lastname TEXT,
    cst_marital_status TEXT,
    cst_gndr TEXT,
    cst_create_date DATE
);


DROP TABLE IF EXISTS bronze.crm_prd_info;

CREATE TABLE IF NOT EXISTS bronze.crm_prd_info(
    prd_id       INTEGER,
    prd_key      TEXT,
    prd_nm       TEXT,
    prd_cost     INTEGER,
    prd_line     TEXT,
    prd_start_dt DATE,
    prd_end_dt   DATE
);


DROP TABLE IF EXISTS bronze.crm_sales_details;

CREATE TABLE IF NOT EXISTS bronze.crm_sales_details(
    sls_ord_num  TEXT,
    sls_prd_key  TEXT,
    sls_cust_id  INTEGER,
    sls_order_dt INTEGER,
    sls_ship_dt  INTEGER,
    sls_due_dt   INTEGER,
    sls_sales    INTEGER,
    sls_quantity INTEGER,
    sls_price    INTEGER
);


DROP TABLE IF EXISTS bronze.erp_cust_az12;

CREATE TABLE IF NOT EXISTS bronze.erp_cust_az12(
    cid    TEXT,
    bdate  DATE,
    gen    TEXT
);


DROP TABLE IF EXISTS bronze.erp_loc_a101;

CREATE TABLE IF NOT EXISTS bronze.erp_loc_a101(
    cid    TEXT,
    cntry  TEXT
);


DROP TABLE IF EXISTS bronze.erp_px_cat_g1v2;

CREATE TABLE IF NOT EXISTS bronze.erp_px_cat_g1v2(
    id           TEXT,
    cat          TEXT,
    subcat       TEXT,
    maintenance  TEXT
);
