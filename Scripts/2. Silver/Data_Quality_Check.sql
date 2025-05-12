/*
=================================================================================================================================
Script Name : Data_Quality_Check.sql
    - Data Quality Checks FROM Bronze TO Silver
=================================================================================================================================

Script Purpose:
    This script performs various quality checks for data consistency, accuracy, and standardizationacross the 'silver' layer.
    It includes checks for:
        - Null or duplicate primary keys.
        - Unwanted spaces in string fields.
        - Data standardization and consistency.
        - Invalid date ranges and orders.
        - Data consistency between related fields.

Usage Notes:
    - Run these checks after data loading Silver Layer.
    - Investigate and resolve any discrepancies found during the checks.
=================================================================================================================================
*/

-- crm_cust_info
-- Finding any duplicates

SELECT cust_id, COUNT(*) AS cust_id_count
FROM bronze.crm_cust_info
GROUP BY cust_id
HAVING COUNT(*) > 1 OR cust_id IS NULL;


-- Unwanted spaces on the data (cst_firstname, cst_lastname, cst_gndr, cst_marital_status)
-- Exception : No results 

SELECT <column_name>
FROM bronze.crm_cust_info
WHERE <column_name> <> TRIM(<column_name>);

-- Data Standardization & consistency easy readability
    -- cst_gender -> M : Male, F : Female, NULL : n/a
    -- cst_marital_status -> S : Single, M -> Married, NULL : n/a

SELECT DISTINCT cst_gndr
FROM bronze.crm_cust_info
UNION ALL
SELECT DISTINCT cst_marital_status
FROM bronze.crm_cust_info

--===============================================================================================================================


-- crm_prd_info
-- Finding any duplicates prd_id, prd_key

SELECT prd_id, COUNT(*) AS prd_id_count
FROM silver.crm_prd_info
GROUP BY prd_id
HAVING COUNT(*) > 1 OR prd_id IS NULL;

-- Unwanted spaces on the data (prd_line)
-- Exception : No results 

SELECT prd_line
FROM silver.crm_prd_info
WHERE prd_line <> TRIM(prd_line);

-- Handled the NULLs in prd_cost column

SELECT prd_cost
FROM silver.crm_prd_info
WHERE prd_cost < 0 OR prd_cost IS NULL;

-- Data Standardization & consistency easy readability
    -- prd_line -> M : Mountain, R : Road, T : Touring, S : Other Sales, NULL : n/a

SELECT DISTINCT prd_line
FROM silver.crm_prd_info

-- Chekcing the invalid dates

SELECT *
FROM silver.crm_prd_info
WHERE prd_start_dt > prd_end_dt

--===============================================================================================================================

-- crm_sales_details

-- finding any unwanted spaces on sls_ord_num, sls_prd_key

SELECT sls_prd_key
FROM silver.crm_sales_details
WHERE sls_prd_key <> TRIM(sls_prd_key);

-- verifying prd_key and cust_id from the sales table and prd_info and cust_info table
-- Expected output : No data

SELECT sls_cust_id
FROM silver.crm_sales_details
WHERE sls_cust_id NOT IN (SELECT cust_id FROM silver.crm_cust_info);

-- validating the dates : sls_order_dt, sls_ship_dt, sls_due_dt

SELECT NULLIF(sls_due_dt, 0) AS sls_due_dt
FROM bronze.crm_sales_details
WHERE sls_due_dt <= 0
    OR LENGTH(sls_due_dt :: TEXT) < 8
    OR sls_order_dt > sls_ship_dt OR sls_order_dt > sls_due_dt
    OR sls_due_dt > 20500101 
    OR sls_due_dt < 19000101;  -- setting a lower and higher boundary to ensure the dates were ranged between them.

-- sls_order_dt - 19
-- sls_ship_dt - clean
-- sls_due_dt - clean

-- validating the sales cloumn = unit * price
    -- if sales is negative, zero or NULL, we can derive it from unit and price
    -- if price is zero or NULL, we can derive it by sales / quantity
    -- price won't be in negative

SELECT sls_sales --sls_sales, sls_quantity, sls_price
FROM silver.crm_sales_details
WHERE sls_sales <= 0 OR sls_sales IS NULL;

-- sls_sales - NULL : 8, 0 : 2, negative : 3
-- sls_quantity - clean
-- sls_price - NULL : 7, negative : 5

--===============================================================================================================================

-- erp_cust_az12

-- transforming the cid to match the cst_key from crm_cust_info

SELECT cid -- CASE WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid, 4) ELSE cid END AS cid
FROM silver.erp_cust_az12
WHERE cid LIKE 'NAS%';

-- finding any unwanted spaces on bdate, gen

SELECT DISTINCT gen FROM silver.erp_cust_az12
WHERE gen NOT IN (SELECT CASE WHEN UPPER(TRIM(gen)) IN ('M', 'MALE') THEN 'Male'
        WHEN UPPER(TRIM(gen)) IN ('F', 'FEMALE') THEN 'Female'
        ELSE 'n\a' END AS gen
FROM bronze.erp_cust_az12);

-- bdate validation

SELECT bdate
FROM silver.erp_cust_az12
WHERE bdate > NOW();

--===============================================================================================================================

-- erp_loc_a101

SELECT REPLACE(cid, '-', '') AS cid
FROM bronze.erp_loc_a101
WHERE REPLACE(cid, '-', '') NOT IN (SELECT cid FROM silver.erp_cust_az12);

SELECT DISTINCT cntry
FROM bronze.erp_loc_a101;

SELECT DISTINCT CASE  
	WHEN TRIM(cntry) = 'DE' THEN 'Germany'
	WHEN TRIM(cntry) IN ('US', 'USA') THEN 'United States'
	WHEN TRIM(cntry) = '' OR TRIM(cntry) IS NULL THEN 'n/a'
	ELSE TRIM(cntry) END AS cntry
FROM bronze.erp_loc_a101;

--===============================================================================================================================

-- erp_px_cat_g1v2

-- finding any mismatch data on prod_cat column

SELECT prd_cat
FROM silver.crm_prd_info
WHERE prd_cat NOT IN (SELECT id FROM bronze.erp_px_cat_g1v2)
UNION
SELECT id
FROM bronze.erp_px_cat_g1v2
WHERE id NOT IN (SELECT prd_cat FROM silver.crm_prd_info);

