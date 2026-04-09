/*
============================================================
🟤 BRONZE LAYER - DATA LOADING PROCEDURE
============================================================

Description:
This stored procedure loads raw data from CRM and ERP source 
CSV files into the Bronze layer tables.

Key Features:
- Truncates existing data before load (full refresh)
- Loads data using BULK INSERT
- Tracks load duration for each table
- Tracks total batch execution time
- Implements TRY-CATCH error handling
- Logs error details (message, number, state, line)

Tables Covered:
CRM:
- bronze.crm_cust_info
- bronze.crm_prd_info
- bronze.crm_sales_details

ERP:
- bronze.erp_cust_az12
- bronze.erp_loc_a101
- bronze.erp_prd_cat_g1v2

Author: Hervin 🚀
============================================================
*/

CREATE OR ALTER PROCEDURE bronze.load_bronze
AS
BEGIN

    DECLARE 
        @Start_time DATETIME,
        @End_time DATETIME,
        @batch_start_time DATETIME,
        @batch_end_time DATETIME;

    BEGIN TRY

        SET @batch_start_time = GETDATE();

        PRINT '============================================';
        PRINT 'LOADING BRONZE LAYER';
        PRINT '=============================================';

        PRINT '---------------------------------------------';
        PRINT 'LOADING CRM TABLES';
        PRINT '----------------------------------------------';


        -- ================= CRM CUSTOMER =================
        SET @Start_time = GETDATE();

        PRINT '>>TRUNCATING TABLE: bronze.crm_cust_info';
        TRUNCATE TABLE bronze.crm_cust_info;

        PRINT '>>INSERTING DATA INTO: bronze.crm_cust_info';
        BULK INSERT bronze.crm_cust_info
        FROM 'E:\sql_datawarehouse_project\source_crm\cust_info.csv'
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ','
        );

        SET @End_time = GETDATE();

        PRINT '>>Load Duration: ' 
        + CAST(DATEDIFF(SECOND, @Start_time, @End_time) AS NVARCHAR(50)) 
        + ' seconds';

        PRINT '----------------------------------';


        -- ================= CRM PRODUCT =================
        SET @Start_time = GETDATE();

        PRINT '>>TRUNCATING TABLE: bronze.crm_prd_info';
        TRUNCATE TABLE bronze.crm_prd_info;

        PRINT '>>INSERTING DATA INTO: bronze.crm_prd_info';
        BULK INSERT bronze.crm_prd_info
        FROM 'E:\sql_datawarehouse_project\source_crm\prd_info.csv'
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ','
        );

        SET @End_time = GETDATE();

        PRINT '>>Load Duration: ' 
        + CAST(DATEDIFF(SECOND, @Start_time, @End_time) AS NVARCHAR(50)) 
        + ' seconds';

        PRINT '----------------------------------';


        -- ================= CRM SALES =================
        SET @Start_time = GETDATE();

        PRINT '>>TRUNCATING TABLE: bronze.crm_sales_details';
        TRUNCATE TABLE bronze.crm_sales_details;

        PRINT '>>INSERTING DATA INTO: bronze.crm_sales_details';
        BULK INSERT bronze.crm_sales_details
        FROM 'E:\sql_datawarehouse_project\source_crm\sales_details.csv'
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ','
        );

        SET @End_time = GETDATE();

        PRINT '>>Load Duration: ' 
        + CAST(DATEDIFF(SECOND, @Start_time, @End_time) AS NVARCHAR(50)) 
        + ' seconds';

        PRINT '----------------------------------';


        PRINT '----------------------------------------------';
        PRINT 'LOADING ERP TABLES';
        PRINT '----------------------------------------------';


        -- ================= ERP CUSTOMER =================
        SET @Start_time = GETDATE();

        PRINT '>>TRUNCATING TABLE: bronze.erp_cust_az12';
        TRUNCATE TABLE bronze.erp_cust_az12;

        PRINT '>>INSERTING DATA INTO: bronze.erp_cust_az12';
        BULK INSERT bronze.erp_cust_az12
        FROM 'E:\sql_datawarehouse_project\source_erp\CUST_AZ12.csv'
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ','
        );

        SET @End_time = GETDATE();

        PRINT '>>Load Duration: ' 
        + CAST(DATEDIFF(SECOND, @Start_time, @End_time) AS NVARCHAR(50)) 
        + ' seconds';

        PRINT '----------------------------------';


        -- ================= ERP LOCATION =================
        SET @Start_time = GETDATE();

        PRINT '>>TRUNCATING TABLE: bronze.erp_loc_a101';
        TRUNCATE TABLE bronze.erp_loc_a101;

        PRINT '>>INSERTING DATA INTO: bronze.erp_loc_a101';
        BULK INSERT bronze.erp_loc_a101
        FROM 'E:\sql_datawarehouse_project\source_erp\LOC_A101.csv'
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ','
        );

        SET @End_time = GETDATE();

        PRINT '>>Load Duration: ' 
        + CAST(DATEDIFF(SECOND, @Start_time, @End_time) AS NVARCHAR(50)) 
        + ' seconds';

        PRINT '----------------------------------';


        -- ================= ERP PRODUCT CATEGORY =================
        SET @Start_time = GETDATE();

        PRINT '>>TRUNCATING TABLE: bronze.erp_prd_cat_g1v2';
        TRUNCATE TABLE bronze.erp_prd_cat_g1v2;

        PRINT '>>INSERTING DATA INTO: bronze.erp_prd_cat_g1v2';
        BULK INSERT bronze.erp_prd_cat_g1v2
        FROM 'E:\sql_datawarehouse_project\source_erp\PX_CAT_G1V2.csv'
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ','
        );

        SET @End_time = GETDATE();

        PRINT '>>Load Duration: ' 
        + CAST(DATEDIFF(SECOND, @Start_time, @End_time) AS NVARCHAR(50)) 
        + ' seconds';

        PRINT '----------------------------------';


        -- ================= TOTAL TIME =================
        SET @batch_end_time = GETDATE();

        PRINT '============================================================';
        PRINT 'LOADING BRONZE IS COMPLETED';

        PRINT 'TOTAL TIME FOR LOADING BRONZE: ' 
        + CAST(DATEDIFF(SECOND, @batch_start_time, @batch_end_time) AS NVARCHAR(50)) 
        + ' seconds';

        PRINT '============================================================';


    END TRY

    BEGIN CATCH

        PRINT '===========================================';
        PRINT 'ERROR OCCURRED DURING BRONZE LAYER';

        PRINT 'Error Message: ' + ERROR_MESSAGE();
        PRINT 'Error Number: ' + CAST(ERROR_NUMBER() AS NVARCHAR(50));
        PRINT 'Error Line: ' + CAST(ERROR_LINE() AS NVARCHAR(50));
        PRINT 'Error State: ' + CAST(ERROR_STATE() AS NVARCHAR(50));

        PRINT '===========================================';

    END CATCH

END;
