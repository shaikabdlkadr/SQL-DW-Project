/*
===============================================================================
Stored Procedure: Load Silver Layer (Bronze -> Silver)
===============================================================================
Script Purpose:
    This stored procedure performs the ETL (Extract, Transform, Load) process to 
    populate the 'silver' schema tables from the 'bronze' schema.
	Actions Performed:
		- Truncates Silver tables.
		- Inserts transformed and cleansed data from Bronze into Silver tables.
		
Parameters:
    None. 
	  This stored procedure does not accept any parameters or return any values.

Usage Example:
    CALL Silver.transform_load_silver();
===============================================================================
*/

CREATE OR REPLACE PROCEDURE silver.transform_load_silver()
LANGUAGE plpgsql
AS $$
DECLARE
    start_time TIMESTAMP;
    end_time TIMESTAMP;
    batch_start_time TIMESTAMP;
    batch_end_time TIMESTAMP;
BEGIN
    batch_start_time := NOW();
    RAISE NOTICE '================================================';
    RAISE NOTICE 'Loading Silver Layer';
    RAISE NOTICE '================================================';

    RAISE NOTICE '------------------------------------------------';
    RAISE NOTICE 'Loading CRM Tables';
    RAISE NOTICE '------------------------------------------------';

    -- Loading the silver.crm_cust_info

    start_time := NOW();
    RAISE NOTICE '>> Truncating Table: silver.crm_cust_info';
    TRUNCATE TABLE silver.crm_cust_info;
    RAISE NOTICE '>> Inserting Data Into: silver.crm_cust_info FROM transformed bronze.crm_cust_info';
    INSERT INTO silver.crm_cust_info

    SELECT cust_id, cst_key, TRIM(cst_firstname) AS cst_firstname, TRIM(cst_lastname) AS cst_lastname, 
    -- Standardize the marriage status to readable format
    CASE
        WHEN UPPER(TRIM(cst_marital_status)) = 'S' THEN 'Single'
        WHEN UPPER(TRIM(cst_marital_status)) = 'M' THEN 'Married'
        ELSE 'n/a' END AS cst_marital_status,
    -- Standardize the gender status to readable format
    CASE
        WHEN UPPER(TRIM(cst_gndr)) = 'M' THEN 'Male'
        WHEN UPPER(TRIM(cst_gndr)) = 'F' THEN 'Female'
        ELSE 'n/a' END AS cst_gndr,
    cst_create_date
    FROM (
        SELECT *, ROW_NUMBER() OVER(PARTITION BY cust_id ORDER BY cst_create_date DESC) AS upd_flag
        FROM bronze.crm_cust_info
        WHERE cust_id IS NOT NULL
        ) subq
    WHERE upd_flag = 1;     -- Selecting the most recent record.
    end_time := NOW();
    RAISE NOTICE '>> Load Duration: % seconds', EXTRACT(SECONDS FROM end_time - start_time);
    RAISE NOTICE '------------------------------------------------';

    
    -- Loading the silver.crm_prd_info

    start_time := NOW();
    RAISE NOTICE '>> Truncating Table: silver.crm_prd_info';
    TRUNCATE TABLE silver.crm_prd_info;
    RAISE NOTICE '>> Inserting Data Into: silver.crm_prd_info FROM transformed bronze.crm_prd_info';

    INSERT INTO silver.crm_prd_info
    SELECT prd_id, REPLACE(SUBSTRING(prd_key, 1, 5), '-', '_') AS prd_cat, -- prodact category extracted and transformed with referenced to prod_cat table
        SUBSTRING(prd_key, 7) AS prd_key_sls,
        prd_nm as prd_name, COALESCE(prd_cost, 0) AS prd_cost, CASE UPPER(TRIM(prd_line))
            WHEN 'M' THEN 'Mountain'
            WHEN 'R' THEN 'Road'
            WHEN 'S' THEN 'Other Sales'
            WHEN 'T' THEN 'Touring'
            ELSE 'n/a' END AS prd_line,
        prd_start_dt, LEAD(prd_start_dt) OVER(PARTITION BY prd_key ORDER BY prd_start_dt) - 1 AS prd_end_dt
    FROM bronze.crm_prd_info;

    end_time := NOW();
    RAISE NOTICE '>> Load Duration: % seconds', EXTRACT(SECONDS FROM end_time - start_time);
    RAISE NOTICE '------------------------------------------------';


    -- Loading the silver.crm_sales_details

    start_time := NOW();
    RAISE NOTICE '>> Truncating Table: silver.crm_sales_details';
    TRUNCATE TABLE silver.crm_sales_details;
    RAISE NOTICE '>> Inserting Data Into: silver.crm_sales_details FROM transformed bronze.crm_sales_details';

    INSERT INTO silver.crm_sales_details
    SELECT sls_ord_num, sls_prd_key, sls_cust_id, CASE
        WHEN sls_order_dt <= 0 OR LENGTH(sls_order_dt :: TEXT) <> 8 ThEN NULL
        ELSE CAST(sls_order_dt :: TEXT AS DATE) END AS sls_order_dt,
        CAST(sls_ship_dt :: TEXT AS DATE) AS sls_ship_dt,
        CAST(sls_due_dt :: TEXT AS DATE) AS sls_due_dt,
        CASE WHEN sls_sales <= 0 OR sls_sales IS NULL OR sls_sales != sls_quantity * ABS(sls_price)
		    THEN sls_quantity * ABS(sls_price)
	        ELSE sls_sales END AS sls_sales, sls_quantity,
	    CASE WHEN sls_price <= 0 OR sls_price IS NULL THEN sls_sales / NULLIF(sls_quantity, 0)
		    ELSE sls_price END AS sls_price
    FROM bronze.crm_sales_details;

    end_time := NOW();

    RAISE NOTICE '>> Load Duration: % seconds', EXTRACT(SECONDS FROM end_time - start_time);
    RAISE NOTICE '------------------------------------------------';


    -- Loading the silver.erp_cust_az12

    start_time := NOW();
    RAISE NOTICE '>> Truncating Table: silver.erp_cust_az12';
    TRUNCATE TABLE silver.erp_cust_az12;
    RAISE NOTICE '>> Inserting Data Into: silver.erp_cust_az12 FROM transformed bronze.erp_cust_az12';

    INSERT INTO silver.erp_cust_az12
    SELECT CASE WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid, 4) ELSE cid END AS cid,
        CASE WHEN bdate > NOW() THEN NULL
            ELSE bdate END AS bdate, 
        CASE WHEN UPPER(TRIM(gen)) IN ('M', 'MALE') THEN 'Male'
            WHEN UPPER(TRIM(gen)) IN ('F', 'FEMALE') THEN 'Female'
            ELSE 'n/a' END AS gen
    FROM bronze.erp_cust_az12;

    end_time := NOW();

    RAISE NOTICE '>> Load Duration: % seconds', EXTRACT(SECONDS FROM end_time - start_time);
    RAISE NOTICE '------------------------------------------------';


    -- loading the silver.erp_loc_a101

    start_time := NOW();
    RAISE NOTICE '>> Truncating Table: silver.erp_loc_a101';
    TRUNCATE TABLE silver.erp_loc_a101;
    RAISE NOTICE '>> Inserting Data Into: silver.erp_loc_a101 FROM transformed bronze.erp_loc_a101';

    INSERT INTO silver.erp_loc_a101
    SELECT REPLACE(cid, '-', '') AS cid,
        CASE WHEN TRIM(cntry) = 'DE' THEN 'Germany'
        WHEN TRIM(cntry) IN ('US', 'USA') THEN 'United States'
        WHEN TRIM(cntry) = '' OR TRIM(cntry) IS NULL THEN 'n/a'
        ELSE TRIM(cntry) END AS cntry
    FROM bronze.erp_loc_a101;

    end_time := NOW();

    RAISE NOTICE '>> Load Duration: % seconds', EXTRACT(SECONDS FROM end_time - start_time);
    RAISE NOTICE '------------------------------------------------';

    -- loading the silver.erp_px_cat_g1v2

    start_time := NOW();
    RAISE NOTICE '>> Truncating Table: silver.erp_px_cat_g1v2';
    TRUNCATE TABLE silver.erp_px_cat_g1v2;
    RAISE NOTICE '>> Inserting Data Into: silver.erp_px_cat_g1v2 FROM transformed bronze.erp_px_cat_g1v2';

    INSERT INTO silver.erp_px_cat_g1v2
    SELECT *
    FROM bronze.erp_px_cat_g1v2;

    end_time := NOW();

    RAISE NOTICE '>> Load Duration: % seconds', EXTRACT(SECONDS FROM end_time - start_time);
    RAISE NOTICE '------------------------------------------------';

    batch_end_time := NOW();

    RAISE NOTICE '==========================================';
    RAISE NOTICE 'Loading Silver Layer is Completed';
    RAISE NOTICE '   - Total Load Duration: % seconds', EXTRACT(SECOND FROM batch_end_time - batch_start_time);
    RAISE NOTICE '==========================================';

EXCEPTION
    WHEN OTHERS THEN
        RAISE NOTICE '==========================================';
        RAISE NOTICE 'ERROR OCCURRED DURING LOADING SILVER LAYER';
        RAISE NOTICE 'Error Message: %', SQLERRM;
        RAISE NOTICE 'Error State: %', SQLSTATE;
        RAISE NOTICE '==========================================';
END;
$$;
