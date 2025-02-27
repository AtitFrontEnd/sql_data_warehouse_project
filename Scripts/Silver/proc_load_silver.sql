/*
***************************************************
Stored Proc: Load Silver Layer (Bronze --> Silver)
***************************************************
Script Purpose: 
      This stored procedure performs the ETL (Extract, Transform, Load) process to 
populate the 'Silver' schema tables from the 'Bronze' schema.

Action Performed:

-Truncate Silver Tables.
- Insert transformed and cleansed data from Bronze into silver tables.

Parameters:
None
This stored procedure does not accept any paramenters or return any vales.
*/


EXEC silver.load_silver

CREATE or ALTER PROCEDURE silver.load_silver AS
BEGIN
----------------------------------------------Table 1---------------------------
	DECLARE @start_time DATETIME, @end_time DATETIME, @batch_start_time DATETIME, @batch_end_time DATETIME;
	BEGIN TRY 

			SET @batch_start_time = GETDATE();
			PRINT '===============================================================';
			PRINT 'loading the Silver layer';
			PRINT '===============================================================';

			PRINT '-----------------------------------------------------';
			PRINT'LOADING CRM Tables';
			PRINT '-----------------------------------------------------';
	
			SET @start_time = GETDATE();
		Print'>>Truncating table: silver.crm_cust_info';
		TRUNCATE TABLE silver.crm_cust_info;
		PRINT '>> Inserting data Into: silver.crm_cust_info'; 
		INSERT INTO silver.crm_cust_info(
			cst_id,
			cst_key,
			cst_firstname,
			cst_lastname,
			cst_material_status,
			cst_gndr,
			cst_create_date,
			dwh_create_date
		)
		select 
			cst_id,
			cst_key,

			---TRIMING, data cleansing - unwanted charaters
			TRIM(cst_firstname) AS cst_firstname,
			TRIM(cst_lastname) AS cst_lastname,

			---Data Normalization/Standardization mapped coded values to meaningful, userfriednly description
			CASE WHEN UPPER(TRIM(cst_material_status)) = 'S' THEN 'Single'
				WHEN UPPER(TRIM(cst_material_status)) = 'M' THEN 'Married'
				ELSE 'n/a'
			END cst_material_status,

			-------Data Normalization/Standardization mapped coded values to meaningful, userfriednly description
			---Hanlding missing values to default value like null to N/A
			CASE WHEN UPPER(TRIM(cst_gndr)) = 'F' THEN 'Female'
				WHEN UPPER(TRIM(cst_gndr)) = 'M' THEN 'Male'
				ELSE 'n/a'
			END cst_gndr,
			cst_create_date,
			getdate()
		FROM bronze.crm_cust_info;
		SET @end_time = GETDATE();
		PRINT '>>LOAD Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' SECONDS';


		------------------------------------Prd Cleaning table 2 from Bronze-----------------------
	
		SET @start_time = GETDATE();
		Print'>>Truncating table: silver.crm_prod_info';
		TRUNCATE TABLE silver.crm_prod_info;
		PRINT '>> Inserting data Into: silver.crm_prod_info'; 

		INSERT INTO silver.crm_prod_info (
			prd_id,
			cat_id,
			prd_key,
			prd_nm,
			prd_cost,
			prd_line,
			prd_start_dt,
			prd_end_dt,
			dwh_create_date
		)
		select 
		prd_id,
		replace(SUBSTRING(prd_key,1,5),'-','_') as cat_id,
		SUBSTRING(prd_key, 7, LEN(prd_key)) as prd_key, 
		prd_nm,
		ISNULL(prd_cost, 0) AS prd_cost,
		CASE WHEN UPPER(TRIM(prd_line)) = 'M' THEN 'Mountain'
			WHEN UPPER(TRIM(prd_line)) = 'R' THEN 'Road'
			WHEN UPPER(TRIM(prd_line)) = 'S' THEN 'Other sales'
			WHEN UPPER(TRIM(prd_line)) = 'T' THEN 'Touring'
			ELSE 'n/a'
		END AS prd_line,
		CAST (prd_start_dt AS DATE) AS prd_start_dt, 
		CAST(LEAD (prd_start_dt) OVER (PARTITION BY prd_key ORDER BY prd_start_dt) -1 AS DATE) AS prd_end_dt,
		getdate()
		FROM bronze.crm_prod_info
		SET @end_time = GETDATE();
		PRINT '>>LOAD Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' SECONDS';
	
	
		---------------------------------------Data Transformation for Table 3---------------------------
	
	
		SET @start_time = GETDATE();
		Print'>>Truncating table: silver.crm_sales_details';
		TRUNCATE TABLE silver.crm_prod_info;
		PRINT '>> Inserting data Into: silver.crm_sales_details'; 
		INSERT INTO silver.crm_sales_details (
			sls_ord_num,
			sls_prd_key,
			sls_cust_id,
			sls_order_dt,
			sls_ship_dt,
			sls_due_dt,
			sls_sales,
			sls_quantity,
			sls_price
		)
		select 
			 sls_ord_num,
			 sls_prd_key,
			 sls_cust_id,
			  CASE	WHEN sls_order_dt = 0 OR LEN(sls_order_dt) != 8 THEN NULL
					ELSE CAST(CAST(sls_order_dt  AS VARCHAR) AS DATE)
			 END AS sls_order_dt,
			 CASE WHEN sls_ship_dt = 0 OR LEN(sls_ship_dt) != 8 THEN NULL
				   ELSE CAST(CAST(sls_ship_dt  AS VARCHAR) AS DATE)
			 END AS sls_ship_dt,
			  CASE WHEN sls_due_dt = 0 OR LEN(sls_due_dt) != 8 THEN NULL
				   ELSE CAST(CAST(sls_due_dt  AS VARCHAR) AS DATE)
			 END AS sls_ship_dt,
			CASE WHEN sls_sales IS NULL OR sls_sales <= 0 OR sls_sales != sls_quantity * ABS(sls_price)
				THEN sls_quantity * ABS(sls_price)
				ELSE sls_sales
			END AS sls_sales,
			 sls_quantity,
			CASE WHEN sls_price IS NULL OR sls_price <= 0
				THEN sls_sales / NULLIF (sls_quantity, 0)
				ELSE sls_price 
			END  AS sls_price
		from bronze.crm_sales_details
		SET @end_time = GETDATE();
		PRINT '>>LOAD Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' SECONDS';
 
		----------------------Tranformation for ERP Table 4 customer AZ12--------------- 
	
		SET @start_time = GETDATE();
		Print'>>Truncating table: silver.erp_cust_az12';
		TRUNCATE TABLE silver.erp_cust_az12;
		PRINT '>> Inserting data Into: silver.erp_cust_az12'; 

		INSERT INTO silver.erp_cust_az12(cid, bdate, gen)

		SELECT 
		CASE WHEN cid like 'NAS%' THEN substring (cid, 4, len(cid)) 
			ELSE cid 
		END as cid,
		CASE WHEN bdate > getdate()  THEN NULL
			ELSE bdate
		END AS bdate, 
		CASE WHEN UPPER(TRIM(gen)) in ('F','FEMALE') THEN 'Female'
			WHEN UPPER(TRIM(gen)) in ('M','MALE') THEN 'Male'
			ELSE 'n/a'
		END as gen
		FROM
		bronze.erp_cust_az12
		SET @end_time = GETDATE();
		PRINT '>>LOAD Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' SECONDS';

		--------------------------------------Tranform ERP Table A101 5-------------
	
	
		SET @end_time = GETDATE();
		Print'>>Truncating table: silver.erp_loc_a101';
		TRUNCATE TABLE silver.erp_loc_a101;
		PRINT '>> Inserting data Into: silver.erp_loc_a101'; 

		INSERT INTO silver.erp_loc_a101(cid, cntry)
		select
		REPLACE(cid, '-', '') cid,
		CASE WHEN TRIM(cntry) = 'DE' THEN 'Germany'  
			WHEN TRIM(cntry) IN ('US','USA') THEN 'United States' 
			WHEN TRIM (cntry)= '' or cntry IS NULL THEN 'n/a'
			ELSE TRIM(cntry)
		END AS cntry
		from bronze.erp_loc_a101
		SET @end_time = GETDATE();
		PRINT '>>LOAD Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' SECONDS';


		-------------------------------------Tranform ERP Table g1v2 6-------------
	
		SET @start_time = GETDATE();
		Print'>>Truncating table: silver.erp_px_cat_g1v2';
		TRUNCATE TABLE silver.erp_px_cat_g1v2;
		PRINT '>> Inserting data Into: silver.erp_px_cat_g1v2'; 

		INSERT INTO silver.erp_px_cat_g1v2(
		id, cat, subcat, maintenance
		)

		select
		id,
		cat,
		subcat,
		maintenance
		from bronze.erp_px_cat_g1v2
		SET @end_time = GETDATE();
		PRINT '>>LOAD Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' SECONDS';

	
		SET @batch_end_time = GETDATE();
		PRINT'======================================='
		PRINT 'LOADING Silver layer is completed';
		PRINT 'total load duration: ' + CAST(DATEDIFF(SECOND, @batch_start_time,  @batch_end_time) AS NVARCHAR) + '  seconds';
		PRINT '=====================================';
		END TRY

		BEGIN CATCH
		PRINT'=======================================';
		PRINT'Error occured during loading bronze layer';
		PRINT'Error message' + ERROR_MESSAGE();
		PRINT 'Error Message' + CAST (ERROR_NUMBER() AS NVARCHAR);
		PRINT 'Error Message' + CAST (ERROR_STATE() AS NVARCHAR);
		PRINT'=======================================';
		END CATCH
END
