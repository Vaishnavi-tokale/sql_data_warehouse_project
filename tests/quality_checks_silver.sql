/*
==========================================================================
Quality Checks
==========================================================================
Script Purpose:
     This script performs various Quality checks for data consistency,accuracy,
     and standardization across the 'silver' schema. It includes checks for:
     - Null or duplicate primary keys.
     - Unwanted spaces in string fields.
     - Data standardization and consistency.
     - Invalid data ranges and orders.
     - Data consistency between related fields.

Usage Notes:
    - Run these checks after data loading Silver layer.
    - Investigate and resolve any discrepancies found during the checks.
==========================================================================
*/

/*
==========================================================================
QUALITY CHECKS FOR SILVER LAYER
==========================================================================

This script performs data quality checks for:
    ✔ Duplicate keys
    ✔ Null primary keys
    ✔ Unwanted spaces
    ✔ Invalid values
    ✔ Data standardization issues
    ✔ Incorrect data transformations
    ✔ Date validation
    ✔ Value consistency (Sales = Quantity × Price)

Run this after your Silver stored procedure.
Investigate any rows returned — they indicate data issues.

==========================================================================
*/

---------------------------------------------------------
-- CRM CUSTOMER INFO QUALITY CHECKS
---------------------------------------------------------
PRINT '>>> Checking silver.crm_cust_info';

-- Check for NULL or duplicate customer IDs
SELECT cst_id, COUNT(*)
FROM silver.crm_cust_info
GROUP BY cst_id
HAVING cst_id IS NULL OR COUNT(*) > 1;

-- Unwanted spaces in names
SELECT *
FROM silver.crm_cust_info
WHERE cst_firstname != TRIM(cst_firstname)
   OR cst_lastname  != TRIM(cst_lastname);

-- Invalid marital status values
SELECT DISTINCT cst_marital_status
FROM silver.crm_cust_info
WHERE cst_marital_status NOT IN ('Single','Married','n/a');

-- Invalid gender
SELECT DISTINCT cst_gndr
FROM silver.crm_cust_info
WHERE cst_gndr NOT IN ('Male','Female','n/a');

---------------------------------------------------------
-- CRM PRODUCT INFO QUALITY CHECKS
---------------------------------------------------------
PRINT '>>> Checking silver.crm_prd_info';

-- Check duplicates
SELECT prd_id, COUNT(*)
FROM silver.crm_prd_info
GROUP BY prd_id
HAVING COUNT(*) > 1;

-- Invalid category IDs
SELECT DISTINCT cat_id
FROM silver.crm_prd_info
WHERE cat_id IS NULL OR cat_id = '';

-- Invalid product line values
SELECT DISTINCT prd_line
FROM silver.crm_prd_info
WHERE prd_line NOT IN ('Mountain','Road','Other Sales','Touring','n/a');

-- Invalid cost values
SELECT *
FROM silver.crm_prd_info
WHERE prd_cost < 0;

---------------------------------------------------------
--CRM SALES DETAILS QUALITY CHECKS
---------------------------------------------------------
PRINT '>>> Checking silver.crm_sales_details';

-- Check primary key issues
SELECT sls_ord_num, sls_prd_key, sls_cust_id, COUNT(*)
FROM silver.crm_sales_details
GROUP BY sls_ord_num, sls_prd_key, sls_cust_id
HAVING COUNT(*) > 1;

-- Date validation
SELECT *
FROM silver.crm_sales_details
WHERE sls_order_dt > sls_ship_dt
   OR sls_ship_dt > sls_due_dt;

-- Sales consistency check
SELECT *
FROM silver.crm_sales_details
WHERE sls_sales != sls_quantity * sls_price
   OR sls_sales < 0;

-- Quantity validation
SELECT *
FROM silver.crm_sales_details
WHERE sls_quantity <= 0;

---------------------------------------------------------
-- ERP CUSTOMER AZ12 QUALITY CHECKS
---------------------------------------------------------
PRINT '>>> Checking silver.erp_cust_az12';

-- NULL or duplicate customer IDs
SELECT cid, COUNT(*)
FROM silver.erp_cust_az12
GROUP BY cid
HAVING cid IS NULL OR COUNT(*) > 1;

-- Invalid future birthdates
SELECT *
FROM silver.erp_cust_az12
WHERE bdate > GETDATE();

-- Invalid gender values
SELECT DISTINCT gen
FROM silver.erp_cust_az12
WHERE gen NOT IN ('Male','Female','n/a');

---------------------------------------------------------
-- ERP LOCATION A101 QUALITY CHECKS
---------------------------------------------------------
PRINT '>>> Checking silver.erp_loc_a101';

-- CID validation
SELECT cid, COUNT(*)
FROM silver.erp_loc_a101
GROUP BY cid
HAVING cid IS NULL OR COUNT(*) > 1;

-- Country code standardization check
SELECT DISTINCT cntry
FROM silver.erp_loc_a101;

---------------------------------------------------------
-- ERP PRODUCT CATEGORY GLV2 QUALITY CHECKS
---------------------------------------------------------
PRINT '>>> Checking silver.erp_px_cat_glv2';

-- Duplicate ID check
SELECT id, COUNT(*)
FROM silver.erp_px_cat_glv2
GROUP BY id
HAVING COUNT(*) > 1;

-- Unwanted spaces
SELECT *
FROM silver.erp_px_cat_glv2
WHERE cat       != TRIM(cat)
   OR subcat    != TRIM(subcat)
   OR maintenance != TRIM(maintenance);

-- Check standardization of maintenance column
SELECT DISTINCT maintenance
FROM silver.erp_px_cat_glv2;

PRINT '>>> QUALITY CHECKS COMPLETED.';
