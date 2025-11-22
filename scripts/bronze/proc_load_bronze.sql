/*
============================================================================
Stored procedure: Load Bronze Layer (Source-> Bronze)
============================================================================
Scripts Purpose:
    This stored procedure loads data into the 'bronze' schema from external CSV files.
    It performs the following actions :
    - Truncate the bronze tables before loading data.
    - Uses the 'BULK INSERT' command to load data from csv files to bronze tables


Paramters:
     None.
  This stored procedure does not accept any parameters or return any values.


Usage Example:
    EXEC bronze.load_bronze;
============================================================================
*/
CREATE OR ALTER PROCEDURE bronze.load_bronze AS
BEGIN
    DECLARE @start_time DATETIME,@end_time DATETIME;
    BEGIN TRY
    PRINT '===================================================';
    PRINT 'Loading Bronze Layer';
    PRINT '===================================================';

    PRINT '---------------------------------------------------';
    PRINT 'Loading CRM Tables';
    PRINT '---------------------------------------------------';

    ---------------------------------------------------
    -- CRM CUST INFO
    ---------------------------------------------------
    SET @start_time = GETDATE();
    PRINT '>> Truncating Table:  bronze.crm_cust_info';
    TRUNCATE TABLE bronze.crm_cust_info;

    PRINT '>> Inserting Data Into:  bronze.crm_cust_info';
    BULK INSERT bronze.crm_cust_info
    FROM 'C:\DW\cust_info.csv'
    WITH (
        FIRSTROW = 2,
        FIELDTERMINATOR = ',',
        ROWTERMINATOR = '\n',
        TABLOCK
    );
    SET @end_time = GETDATE();
    PRINT '>> Load Duration: ' +CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
    PRINT '>> -------------';
    SELECT COUNT(*) AS crm_cust_info_count
    FROM bronze.crm_cust_info;

    ---------------------------------------------------
    -- CRM PRODUCT INFO
    ---------------------------------------------------
    SET @start_time = GETDATE();
    PRINT '>> Truncating Table:  bronze.crm_prd_info';
    TRUNCATE TABLE bronze.crm_prd_info;

    PRINT '>> Inserting Data Into:  bronze.crm_prd_info';
    BULK INSERT bronze.crm_prd_info
    FROM 'C:\DW\prd_info.csv'
    WITH (
        FIRSTROW = 2,
        FIELDTERMINATOR = ',',
        ROWTERMINATOR = '\n',
        TABLOCK
    );
     SET @end_time = GETDATE();
    PRINT '>> Load Duration: ' +CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
    PRINT '>> -------------';

    SELECT COUNT(*) AS crm_prd_info_count
    FROM bronze.crm_prd_info;

    ---------------------------------------------------
    -- CRM SALES DETAILS
    ---------------------------------------------------

    SET @start_time = GETDATE();
    PRINT '>> Truncating Table:  bronze.crm_sales_details';
    TRUNCATE TABLE bronze.crm_sales_details;

    PRINT '>> Inserting Data Into:  bronze.crm_sales_details';
    BULK INSERT bronze.crm_sales_details
    FROM 'C:\DW\sales_details.csv'
    WITH (
        FIRSTROW = 2,
        FIELDTERMINATOR = ',',
        ROWTERMINATOR = '\n',
        TABLOCK
    );
     SET @end_time = GETDATE();
    PRINT '>> Load Duration: ' +CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
    PRINT '>> -------------';

    SELECT COUNT(*) AS crm_sales_details_count
    FROM bronze.crm_sales_details;

    ---------------------------------------------------
    PRINT '---------------------------------------------------';
    PRINT 'Loading ERP Tables';
    PRINT '---------------------------------------------------';

    ---------------------------------------------------
    -- ERP CUST AZ12
    ---------------------------------------------------
    SET @start_time = GETDATE();
    PRINT '>> Truncating Table:  bronze.erp_cust_az12';
    TRUNCATE TABLE bronze.erp_cust_az12;

    PRINT '>> Inserting Data Into:  bronze.erp_cust_az12';
    BULK INSERT bronze.erp_cust_az12
    FROM 'C:\DW\CUST_AZ12.csv'
    WITH (
        FIRSTROW = 2,
        FIELDTERMINATOR = ',',
        ROWTERMINATOR = '\n',
        TABLOCK
    );
    SET @end_time = GETDATE();
    PRINT '>> Load Duration: ' +CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
    PRINT '>> -------------';
    SELECT COUNT(*) AS erp_cust_az12_count
    FROM bronze.erp_cust_az12;

    ---------------------------------------------------
    -- ERP LOC A101
    ---------------------------------------------------
    SET @start_time = GETDATE();
    PRINT '>> Truncating Table:  bronze.erp_loc_a101';
    TRUNCATE TABLE bronze.erp_loc_a101;

    PRINT '>> Inserting Data Into:  bronze.erp_loc_a101';
    BULK INSERT bronze.erp_loc_a101
    FROM 'C:\DW\LOC_A101.csv'
    WITH (
        FIRSTROW = 2,
        FIELDTERMINATOR = ',',
        ROWTERMINATOR = '\n',
        TABLOCK
    );
    SET @end_time = GETDATE();
    PRINT '>> Load Duration: ' +CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
    PRINT '>> -------------';
    SELECT COUNT(*) AS erp_loc_a101_count
    FROM bronze.erp_loc_a101;

    ---------------------------------------------------
    -- ERP PX CAT GLV2
    ---------------------------------------------------
    SET @start_time = GETDATE();
    PRINT '>> Truncating Table:  bronze.erp_px_cat_glv2';
    TRUNCATE TABLE bronze.erp_px_cat_glv2;

    PRINT '>> Inserting Data Into:  bronze.erp_px_cat_glv2';
    BULK INSERT bronze.erp_px_cat_glv2
    FROM 'C:\DW\PX_CAT_G1V2.csv'
    WITH (
        FIRSTROW = 2,
        FIELDTERMINATOR = ',',
        ROWTERMINATOR = '\n',
        TABLOCK
    );
    SET @end_time = GETDATE();
    PRINT '>> Load Duration: ' +CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
    PRINT '>> -------------';

    SELECT COUNT(*) AS erp_px_cat_glv2_count
    FROM bronze.erp_px_cat_glv2;

END TRY
 BEGIN CATCH
    PRINT '============================'
    PRINT 'ERROR OCCURED DURING LOADING BRONZE LAYER'
    PRINT 'ERROR Message' + ERROR_MESSAGE();
    PRINT 'ERROR Message' + CAST (ERROR_NUMBER() AS NVARCHAR);
    PRINT 'ERROR Message' + CAST (ERROR_STATE() AS NVARCHAR);
    PRINT '============================'
 END CATCH
END
