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
    EXEC Silver.load_silver;
===============================================================================
*/


CREATE OR ALTER PROCEDURE silver.load_silver
AS
BEGIN

    DECLARE 
        @start_time DATETIME,
        @end_time DATETIME,
        @batch_start_time DATETIME,
        @batch_end_time DATETIME;

    BEGIN TRY

        SET @batch_start_time = GETDATE();

        --------------------------------------------------
        -- CRM CUSTOMER
        --------------------------------------------------
        SET @start_time = GETDATE();

        TRUNCATE TABLE silver.crm_cust_info;

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
                WHEN UPPER(cst_material_status)='M' THEN 'Married'
                WHEN UPPER(cst_material_status)='S' THEN 'Single'
                ELSE 'n/a'
            END,
            CASE 
                WHEN UPPER(cst_gndr)='M' THEN 'Male'
                WHEN UPPER(cst_gndr)='F' THEN 'Female'
                ELSE 'n/a'
            END,
            cst_create_date
        FROM (
            SELECT *, ROW_NUMBER() OVER (PARTITION BY cst_id ORDER BY cst_create_date DESC) AS rn
            FROM bronze.crm_cust_info
        ) t
        WHERE rn = 1;

        SET @end_time = GETDATE();

        SELECT 'crm_cust_info' AS table_name,
               DATEDIFF(SECOND,@start_time,@end_time) AS load_time_seconds;

        --------------------------------------------------
        -- CRM PRODUCT
        --------------------------------------------------
        SET @start_time = GETDATE();

        TRUNCATE TABLE silver.crm_prd_info;

        INSERT INTO silver.crm_prd_info (
            prd_id, cat_id, prd_key, prd_nm,
            prd_cost, prd_line, prd_start_dt, prd_end_dt
        )
        SELECT 
            prd_id,
            REPLACE(SUBSTRING(prd_key,1,5),'-','_'),
            SUBSTRING(prd_key,7,LEN(prd_key)),
            prd_nm,
            ISNULL(prd_cost,0),
            CASE 
                WHEN UPPER(TRIM(prd_line))='R' THEN 'Road'
                WHEN UPPER(TRIM(prd_line))='S' THEN 'Others sales'
                WHEN UPPER(TRIM(prd_line))='M' THEN 'Mountains'
                WHEN UPPER(TRIM(prd_line))='T' THEN 'Touring'
                ELSE 'n/a'
            END,
            CAST(prd_start_dt AS DATE),
            DATEADD(DAY,-1,
                LEAD(TRY_CONVERT(DATE,prd_start_dt))
                OVER(PARTITION BY prd_key ORDER BY TRY_CONVERT(DATE,prd_start_dt))
            )
        FROM bronze.crm_prd_info;

        SET @end_time = GETDATE();

        SELECT 'crm_prd_info' AS table_name,
               DATEDIFF(SECOND,@start_time,@end_time) AS load_time_seconds;

        --------------------------------------------------
        -- CRM SALES
        --------------------------------------------------
        SET @start_time = GETDATE();

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
            TRY_CONVERT(DATE,CAST(sls_order_dt AS VARCHAR(8)),112),
            TRY_CONVERT(DATE,CAST(sls_ship_dt AS VARCHAR(8)),112),
            TRY_CONVERT(DATE,CAST(sls_due_dt AS VARCHAR(8)),112),
            CAST(
                CASE 
                    WHEN sls_sales IS NULL OR sls_sales<=0 OR sls_sales<>sls_quantity*ABS(sls_price)
                    THEN sls_quantity*ABS(sls_price)
                    ELSE sls_sales
                END AS DECIMAL(10,0)
            ),
            sls_quantity,
            CAST(
                CASE 
                    WHEN sls_price IS NULL OR sls_price<=0
                    THEN sls_sales/NULLIF(sls_quantity,0)
                    ELSE sls_price
                END AS DECIMAL(10,0)
            )
        FROM bronze.crm_sales_details;

        SET @end_time = GETDATE();

        SELECT 'crm_sales_details' AS table_name,
               DATEDIFF(SECOND,@start_time,@end_time) AS load_time_seconds;

        --------------------------------------------------
        -- ERP CUSTOMER
        --------------------------------------------------
        SET @start_time = GETDATE();

        TRUNCATE TABLE silver.erp_cust_az12;

        INSERT INTO silver.erp_cust_az12 (cid,bdate,gen)
        SELECT 
            REPLACE(cid,'NASAW000',''),
            CASE WHEN bdate>GETDATE() THEN NULL ELSE bdate END,
            CASE 
                WHEN UPPER(TRIM(gen)) IN ('M','MALE') THEN 'Male'
                WHEN UPPER(TRIM(gen)) IN ('F','FEMALE') THEN 'Female'
                ELSE 'n/a'
            END
        FROM bronze.erp_cust_az12;

        SET @end_time = GETDATE();

        SELECT 'erp_cust_az12' AS table_name,
               DATEDIFF(SECOND,@start_time,@end_time) AS load_time_seconds;

        --------------------------------------------------
        -- ERP LOCATION
        --------------------------------------------------
        SET @start_time = GETDATE();

        TRUNCATE TABLE silver.erp_loc_a101;

        INSERT INTO silver.erp_loc_a101 (cid,cntry)
        SELECT 
            REPLACE(cid,'-',''),
            CASE 
                WHEN UPPER(TRIM(cntry)) IN ('US','USA','UNITED STATES') THEN 'United States'
                WHEN UPPER(TRIM(cntry)) IN ('DE','GERMANY') THEN 'Germany'
                WHEN UPPER(TRIM(cntry)) IN ('UK','UNITED KINGDOM') THEN 'United Kingdom'
                WHEN UPPER(TRIM(cntry))='CANADA' THEN 'Canada'
                WHEN UPPER(TRIM(cntry))='FRANCE' THEN 'France'
                WHEN UPPER(TRIM(cntry))='AUSTRALIA' THEN 'Australia'
                ELSE 'n/a'
            END
        FROM bronze.erp_loc_a101;

        SET @end_time = GETDATE();

        SELECT 'erp_loc_a101' AS table_name,
               DATEDIFF(SECOND,@start_time,@end_time) AS load_time_seconds;

        --------------------------------------------------
        -- ERP PRODUCT CATEGORY
        --------------------------------------------------
        SET @start_time = GETDATE();

        TRUNCATE TABLE silver.erp_px_cat_g1v2;

        INSERT INTO silver.erp_px_cat_g1v2 (id, cat, subcat, maintenance)
        SELECT id, cat, subcat, maintenance 
        FROM bronze.erp_prd_cat_g1v2;

        SET @end_time = GETDATE();

        SELECT 'erp_px_cat_g1v2' AS table_name,
               DATEDIFF(SECOND,@start_time,@end_time) AS load_time_seconds;

        --------------------------------------------------
        -- TOTAL LOAD TIME
        --------------------------------------------------
        SET @batch_end_time = GETDATE();

        SELECT 'TOTAL_LOAD_TIME' AS process,
               DATEDIFF(SECOND,@batch_start_time,@batch_end_time) AS total_seconds;

    END TRY

    BEGIN CATCH
        SELECT ERROR_MESSAGE() AS error_message;
    END CATCH

END;
