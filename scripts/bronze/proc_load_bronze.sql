/*
===============================================================================
Stored Procedure: Load Bronze Layer (Source -> Bronze)
===============================================================================
Script Purpose:
    This stored procedure loads data into the 'bronze' schema from external CSV files. 
    It performs the following actions:
    - Truncates the bronze tables before loading data.
    - Uses the `BULK INSERT` command to load data from csv Files to bronze tables.

Indication:
	- Step 1: Identify Your Data Paths
	- Step 2: Update the File Paths
	- The Setting of the Sources Paths is on line 35 for the source_crm and the line 36 for the source_erp.


Parameters:
    None. 
	  This stored procedure does not accept any parameters or return any values.

Usage Example:
    EXEC bronze.load_bronze;
===============================================================================
*/

CREATE OR ALTER PROCEDURE bronze.load_bronze AS
BEGIN
    DECLARE @batch_start_time DATETIME, @batch_end_time DATETIME,
            @start_time DATETIME, @end_time DATETIME;
    DECLARE @crm_source NVARCHAR(200), @erp_source NVARCHAR(200);
    DECLARE @sql NVARCHAR(MAX);

    BEGIN TRY
        SET @batch_start_time = GETDATE();
        SET @crm_source = N'Path\To\The\datasets\source_crm\';
        SET @erp_source = N'Path\To\The\datasets\source_erp\';

        PRINT '================================================================';
        PRINT 'Loading Bronze Layer';
        PRINT '================================================================';

        PRINT '----------------------------------------------------------------';
        PRINT 'Loading CRM Tables';
        PRINT '----------------------------------------------------------------';

        -- crm_cust_info
        SET @start_time = GETDATE();
        PRINT '>> Truncating Table: bronze.crm_cust_info';
        TRUNCATE TABLE bronze.crm_cust_info;
        PRINT '>> Inserting Data Into: bronze.crm_cust_info';
        SET @sql = N'BULK INSERT bronze.crm_cust_info FROM ''' + @crm_source + N'cust_info.csv'' WITH (FIRSTROW=2, FIELDTERMINATOR='','', TABLOCK)';
        EXEC(@sql);
        SET @end_time = GETDATE();
        PRINT '>> Load duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
        PRINT '>> ---------------';

        -- crm_prd_info
        SET @start_time = GETDATE();
        PRINT '>> Truncating Table: bronze.crm_prd_info';
        TRUNCATE TABLE bronze.crm_prd_info;
        PRINT '>> Inserting Data Into: bronze.crm_prd_info';
        SET @sql = N'BULK INSERT bronze.crm_prd_info FROM ''' + @crm_source + N'prd_info.csv'' WITH (FIRSTROW=2, FIELDTERMINATOR='','', TABLOCK)';
        EXEC(@sql);
        SET @end_time = GETDATE();
        PRINT '>> Load duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
        PRINT '>> ---------------';

        -- crm_sales_details
        SET @start_time = GETDATE();
        PRINT '>> Truncating Table: bronze.crm_sales_details';
        TRUNCATE TABLE bronze.crm_sales_details;
        PRINT '>> Inserting Data Into: bronze.crm_sales_details';
        SET @sql = N'BULK INSERT bronze.crm_sales_details FROM ''' + @crm_source + N'sales_details.csv'' WITH (FIRSTROW=2, FIELDTERMINATOR='','', TABLOCK)';
        EXEC(@sql);
        SET @end_time = GETDATE();
        PRINT '>> Load duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
        PRINT '>> ---------------';

        PRINT '----------------------------------------------------------------';
        PRINT 'Loading ERP Tables';
        PRINT '----------------------------------------------------------------';

        -- erp_cust_az12
        SET @start_time = GETDATE();
        PRINT '>> Truncating Table: bronze.erp_cust_az12';
        TRUNCATE TABLE bronze.erp_cust_az12;
        PRINT '>> Inserting Data Into: bronze.erp_cust_az12';
        SET @sql = N'BULK INSERT bronze.erp_cust_az12 FROM ''' + @erp_source + N'CUST_AZ12.csv'' WITH (FIRSTROW=2, FIELDTERMINATOR='','', TABLOCK)';
        EXEC(@sql);
        SET @end_time = GETDATE();
        PRINT '>> Load duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
        PRINT '>> ---------------';

        -- erp_loc_a101
        SET @start_time = GETDATE();
        PRINT '>> Truncating Table: bronze.erp_loc_a101';
        TRUNCATE TABLE bronze.erp_loc_a101;
        PRINT '>> Inserting Data Into: bronze.erp_loc_a101';
        SET @sql = N'BULK INSERT bronze.erp_loc_a101 FROM ''' + @erp_source + N'LOC_A101.csv'' WITH (FIRSTROW=2, FIELDTERMINATOR='','', TABLOCK)';
        EXEC(@sql);
        SET @end_time = GETDATE();
        PRINT '>> Load duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
        PRINT '>> ---------------';

        -- erp_px_cat_g1v2
        SET @start_time = GETDATE();
        PRINT '>> Truncating Table: bronze.erp_px_cat_g1v2';
        TRUNCATE TABLE bronze.erp_px_cat_g1v2;
        PRINT '>> Inserting Data Into: bronze.erp_px_cat_g1v2';
        SET @sql = N'BULK INSERT bronze.erp_px_cat_g1v2 FROM ''' + @erp_source + N'PX_CAT_G1V2.csv'' WITH (FIRSTROW=2, FIELDTERMINATOR='','', TABLOCK)';
        EXEC(@sql);
        SET @end_time = GETDATE();
        PRINT '>> Load duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
        PRINT '>> ---------------';

        SET @batch_end_time = GETDATE();
        PRINT '================================================================';
        PRINT 'Loading Bronze Layer is Completed';
        PRINT '>> - Total Load duration: ' + CAST(DATEDIFF(second, @batch_start_time, @batch_end_time) AS NVARCHAR) + ' seconds';
        PRINT '================================================================';

    END TRY
    BEGIN CATCH
        PRINT '================================================================';
        PRINT 'ERROR OCCURED DURING LOADING BRONZE LAYER';
        PRINT 'Error Message: ' + ERROR_MESSAGE();
        PRINT 'Error Number: '  + CAST(ERROR_NUMBER() AS NVARCHAR);
        PRINT 'Error State: '   + CAST(ERROR_STATE() AS NVARCHAR);
        PRINT '================================================================';
    END CATCH
END
