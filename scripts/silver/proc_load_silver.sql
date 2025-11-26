/*
========================================================================
Stored Procedure: Load Silver Layer (Bronze->Silver)
========================================================================
Script Purpose:
  This Stored Procedure performs the ETL (Extract, Transform, Load) process to populate the 'silver' schema tables from the 'bronze' schema.
  populate the 'silver' schema tables from the 'bronze' schema.
Actions Performed:
   - Truncates Silver tables.
   - Inserts transformed and cleansed data from Bronze Into Silver tables.

Parameters:
     None.
     This stored procedure does not accept any paramters or return any values.

Usage Example:
     EXEC silver.load_silver;
=========================================================================
*/
  
CREATE OR ALTER PROCEDURE sp_load_silver_layer
AS
BEGIN
    SET NOCOUNT ON;

    ---------------------------------------------------------------------
    -- Track total procedure start time
    ---------------------------------------------------------------------
    DECLARE @StartTime DATETIME2 = SYSDATETIME();
    DECLARE @StepStart DATETIME2;
    DECLARE @Duration NVARCHAR(100);

    PRINT '=============================================';
    PRINT '>>> Starting Silver Layer Processing...';
    PRINT '=============================================';

    ---------------------------------------------------------------------
    -- STEP 1: Load silver.crm_cust_info
    ---------------------------------------------------------------------
    BEGIN TRY
        SET @StepStart = SYSDATETIME();
        PRINT '>>> Step 1: Truncating silver.crm_cust_info...';

        TRUNCATE TABLE silver.crm_cust_info;
        PRINT '    Table truncated.';

        PRINT '>>> Step 1: Inserting cleaned customer info...';

        INSERT INTO silver.crm_cust_info (
            cst_id, cst_key, cst_firstname, cst_lastname,
            cst_marital_status, cst_gndr, cst_create_date
        )
        SELECT  
            cst_id,
            cst_key,
            TRIM(cst_firstname),
            TRIM(cst_lastname),
            CASE 
                WHEN UPPER(TRIM(cst_marital_status)) = 'M' THEN 'Married'
                WHEN UPPER(TRIM(cst_marital_status)) = 'S' THEN 'Single'
                ELSE 'n/a'
            END,
            CASE 
                WHEN UPPER(TRIM(cst_gndr)) = 'F' THEN 'Female'
                WHEN UPPER(TRIM(cst_gndr)) = 'M' THEN 'Male'
                ELSE 'n/a'
            END,
            cst_create_date
        FROM (
            SELECT *,
                ROW_NUMBER() OVER (PARTITION BY cst_id ORDER BY cst_create_date DESC) AS rn
            FROM bronze.crm_cust_info
        ) t
        WHERE rn = 1 AND cst_id IS NOT NULL;

        SET @Duration = CONCAT(DATEDIFF(SECOND, @StepStart, SYSDATETIME()), ' seconds');
        PRINT '>>> Step 1 Completed. Duration = ' + @Duration;
    END TRY
    BEGIN CATCH
        PRINT '!!! ERROR in Step 1 (Customer Info) !!!';
        PRINT ERROR_MESSAGE();
        RETURN;
    END CATCH;


    ---------------------------------------------------------------------
    -- STEP 2: Load silver.crm_prd_info
    ---------------------------------------------------------------------
    BEGIN TRY
        SET @StepStart = SYSDATETIME();
        PRINT '>>> Step 2: Loading Product Info...';

        TRUNCATE TABLE silver.crm_prd_info;

        INSERT INTO silver.crm_prd_info (
            prd_id, cat_id, prd_key, prd_nm, prd_cost,
            prd_line, prd_start_dt, prd_end_dt
        )
        SELECT
            prd_id,
            REPLACE(SUBSTRING(prd_key, 1, 5), '-', '_'),
            SUBSTRING(prd_key, 7, LEN(prd_key)),
            prd_nm,
            ISNULL(prd_cost, 0),
            CASE UPPER(TRIM(prd_line))
                WHEN 'M' THEN 'Mountain'
                WHEN 'R' THEN 'Road'
                WHEN 'S' THEN 'Other Sales'
                WHEN 'T' THEN 'Touring'
                ELSE 'n/a'
            END,
            CAST(prd_start_dt AS DATE),
            CAST(LEAD(prd_start_dt) OVER (PARTITION BY prd_key ORDER BY prd_start_dt) - 1 AS DATE)
        FROM bronze.crm_prd_info;

        SET @Duration = CONCAT(DATEDIFF(SECOND, @StepStart, SYSDATETIME()), ' seconds');
        PRINT '>>> Step 2 Completed. Duration = ' + @Duration;
    END TRY
    BEGIN CATCH
        PRINT '!!! ERROR in Step 2 (Product Info) !!!';
        PRINT ERROR_MESSAGE();
        RETURN;
    END CATCH;


    ---------------------------------------------------------------------
    -- STEP 3: Load silver.crm_sales_details
    ---------------------------------------------------------------------
    BEGIN TRY
        SET @StepStart = SYSDATETIME();
        PRINT '>>> Step 3: Loading Sales Details...';

        TRUNCATE TABLE silver.crm_sales_details;

        INSERT INTO silver.crm_sales_details (
            sls_ord_num, sls_prd_key, sls_cust_id,
            sls_order_dt, sls_ship_dt, sls_due_dt,
            sls_sales, sls_quantity, sls_price
        )
        SELECT
            sls_ord_num,
            sls_prd_key,
            sls_cust_id,

            CASE WHEN sls_order_dt = 0 OR LEN(sls_order_dt) != 8 THEN NULL
                 ELSE CAST(CAST(sls_order_dt AS VARCHAR) AS DATE)
            END,
            CASE WHEN sls_ship_dt = 0 OR LEN(sls_ship_dt) != 8 THEN NULL
                 ELSE CAST(CAST(sls_ship_dt AS VARCHAR) AS DATE)
            END,
            CASE WHEN sls_due_dt = 0 OR LEN(sls_due_dt) != 8 THEN NULL
                 ELSE CAST(CAST(sls_due_dt AS VARCHAR) AS DATE)
            END,

            CASE WHEN sls_sales IS NULL OR sls_sales <= 0 
                      OR sls_sales != sls_quantity * ABS(sls_price)
                 THEN sls_quantity * ABS(sls_price)
                 ELSE sls_sales
            END,

            sls_quantity,

            CASE WHEN sls_price IS NULL OR sls_price <= 0
                 THEN ABS(sls_price)
                 ELSE sls_price
            END
        FROM bronze.crm_sales_details;

        SET @Duration = CONCAT(DATEDIFF(SECOND, @StepStart, SYSDATETIME()), ' seconds');
        PRINT '>>> Step 3 Completed. Duration = ' + @Duration;
    END TRY
    BEGIN CATCH
        PRINT '!!! ERROR in Step 3 (Sales Details) !!!';
        PRINT ERROR_MESSAGE();
        RETURN;
    END CATCH;


    ---------------------------------------------------------------------
    -- STEP 4: Load silver.erp_cust_az12
    ---------------------------------------------------------------------
    BEGIN TRY
        SET @StepStart = SYSDATETIME();
        PRINT '>>> Step 4: Loading ERP Customer A12...';

        TRUNCATE TABLE silver.erp_cust_az12;

        INSERT INTO silver.erp_cust_az12 (cid, bdate, gen)
        SELECT
            CASE WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid, 4, LEN(cid)) ELSE cid END,
            CASE WHEN bdate > GETDATE() THEN NULL ELSE bdate END,
            CASE 
                WHEN UPPER(TRIM(gen)) IN ('F','FEMALE') THEN 'Female'
                WHEN UPPER(TRIM(gen)) IN ('M','MALE') THEN 'Male'
                ELSE 'n/a'
            END
        FROM bronze.erp_cust_az12;

        SET @Duration = CONCAT(DATEDIFF(SECOND, @StepStart, SYSDATETIME()), ' seconds');
        PRINT '>>> Step 4 Completed. Duration = ' + @Duration;
    END TRY
    BEGIN CATCH
        PRINT '!!! ERROR in Step 4 (ERP A12) !!!';
        PRINT ERROR_MESSAGE();
        RETURN;
    END CATCH;


    ---------------------------------------------------------------------
    -- STEP 5: Load silver.erp_loc_a101
    ---------------------------------------------------------------------
    BEGIN TRY
        SET @StepStart = SYSDATETIME();
        PRINT '>>> Step 5: Loading ERP Location A101...';

        TRUNCATE TABLE silver.erp_loc_a101;

        INSERT INTO silver.erp_loc_a101 (cid, cntry)
        SELECT
            REPLACE(cid, '-', ''),
            CASE 
                WHEN TRIM(cntry) = 'DE' THEN 'Germany'
                WHEN TRIM(cntry) IN ('US','USA') THEN 'United States'
                WHEN cntry IS NULL OR TRIM(cntry) = '' THEN 'n/a'
                ELSE TRIM(cntry)
            END
        FROM bronze.erp_loc_a101;

        SET @Duration = CONCAT(DATEDIFF(SECOND, @StepStart, SYSDATETIME()), ' seconds');
        PRINT '>>> Step 5 Completed. Duration = ' + @Duration;
    END TRY
    BEGIN CATCH
        PRINT '!!! ERROR in Step 5 (ERP Location) !!!';
        PRINT ERROR_MESSAGE();
        RETURN;
    END CATCH;


    ---------------------------------------------------------------------
    -- STEP 6: Load silver.erp_px_cat_glv2
    ---------------------------------------------------------------------
    BEGIN TRY
        SET @StepStart = SYSDATETIME();
        PRINT '>>> Step 6: Loading ERP Category GLV2...';

        TRUNCATE TABLE silver.erp_px_cat_glv2;

        INSERT INTO silver.erp_px_cat_glv2 (id, cat, subcat, maintenance)
        SELECT
            id,
            TRIM(cat),
            TRIM(subcat),
            TRIM(maintenance)
        FROM bronze.erp_px_cat_glv2;

        SET @Duration = CONCAT(DATEDIFF(SECOND, @StepStart, SYSDATETIME()), ' seconds');
        PRINT '>>> Step 6 Completed. Duration = ' + @Duration;
    END TRY
    BEGIN CATCH
        PRINT '!!! ERROR in Step 6 (ERP GLV2) !!!';
        PRINT ERROR_MESSAGE();
        RETURN;
    END CATCH;


    ---------------------------------------------------------------------
    -- TOTAL DURATION
    ---------------------------------------------------------------------
    DECLARE @TotalDuration NVARCHAR(100) =
        CONCAT(DATEDIFF(SECOND, @StartTime, SYSDATETIME()), ' seconds');

    PRINT '=============================================';
    PRINT '>>> Silver Layer Successfully Completed!';
    PRINT '>>> TOTAL TIME: ' + @TotalDuration;
    PRINT '=============================================';

END;
GO
