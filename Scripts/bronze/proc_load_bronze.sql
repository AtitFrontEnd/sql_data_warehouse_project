/*
Stored Procedure: load Bronze (Source -> Bronze)

Script Purpose:

 this stored procedure loads data into the 'bronze' schema from external csv files.

it performs the following action:
- truncate the bronze table before loading the data.
- uses the BUL INSERT command to load data from csv files to bronze tables.

Parameters:
None:
this stored procedure does not accept any parameters or return an values.

Using Example:
 ECEX bronze.load_bronze;
*/




EXEC bronze.load_bronze 
----------------------------Bulk insert for customer info------------------------------------------------------------
Create or alter procedure bronze.load_bronze AS
BEGIN
	DECLARE @start_time DATETIME, @end_time DATETIME, @batch_start_time DATETIME, @batch_end_time DATETIME;
	BEGIN TRY
		 
		SET @batch_start_time = GETDATE();
		PRINT '===============================================================';
		PRINT 'loading the bronze layer';
		PRINT '===============================================================';

		PRINT '-----------------------------------------------------';
		PRINT'LOADING CRM Tables';
		PRINT '-----------------------------------------------------';

		SET @start_time = GETDATE();
		PRINT '>>Truncating Table: bronze.crm.cust.info'
		TRUNCATE TABLE bronze.crm_cust_info;
		------------BULK INSERT------

		PRINT '>>Inserting Data into Table: bronze.crm.cust.info'
		BULK INSERT bronze.crm_cust_info
		from 'C:\Users\lovel\Downloads\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_crm\cust_info.csv'
		with (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT '>>LOAD Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' SECONDS';
		------------------------------------------BULK INSERT--table 2 prod----
		SET @start_time = GETDATE();
		PRINT '>>Truncating Table: bronze.crm_prod_info';
		TRUNCATE TABLE bronze.crm_prod_info;

		PRINT '>>Inserting Data into Table: bronze.crm_prod_info';
		BULK INSERT bronze.crm_prod_info
		from 'C:\Users\lovel\Downloads\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_crm\prd_info.csv'
		with (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		select count(*) from bronze.crm_prod_info
		SET @end_time = GETDATE();
		PRINT '>>LOAD Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' SECONDS';

		------------------------------------------BULK Insert----table 3------------------
		SET @start_time = GETDATE();
		PRINT '>>Truncating Table: bronze.crm_sales_details';
		TRUNCATE TABLE bronze.crm_sales_details ;

		PRINT '>>Inserting Data into Table: bronze.crm_sales_details';
		BULK INSERT bronze.crm_sales_details 
		from 'C:\Users\lovel\Downloads\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_crm\sales_details.csv'
		with(
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);

		SET @end_time = GETDATE();
		PRINT '>>LOAD Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' SECONDS';

		------------------------------------------Bulk Insert TABLE 4-----------------

		PRINT '-----------------------------------------------------';
		PRINT'LOADING ERP Tables';
		PRINT '-----------------------------------------------------';

		SET @start_time = GETDATE();
		PRINT '>>Truncating Table: bronze.erp_loc_a101';
		TRUNCATE TABLE bronze.erp_loc_a101;

		PRINT '>>Inserting Data into Table: bronze.erp_loc_a101';
		BULK INSERT bronze.erp_loc_a101
		from 'C:\Users\lovel\Downloads\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_erp\LOC_A101.csv'
		with(
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT '>>LOAD Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' SECONDS';
		------------------------------------------Bulk Insert TABLE 5-----------------------------------
		SET @start_time = GETDATE();
		PRINT '>>Truncating Table: bronze.erp_cust_az12';
		TRUNCATE TABLE bronze.erp_cust_az12;

		PRINT '>>Inserting Data into Table: bronze.erp_cust_az12';
		BULK INSERT bronze.erp_cust_az12
		from 'C:\Users\lovel\Downloads\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_erp\CUST_AZ12.csv'
		with(
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT '>>LOAD Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' SECONDS';
		------------------------------------------Bulk Insert TABLE 6---------------------------------
		SET @start_time = GETDATE();
		PRINT '>>Truncating Table: bronze.erp_px_cat_g1v2';
		TRUNCATE TABLE bronze.erp_px_cat_g1v2;

		PRINT '>>Inserting Data into Table: bronze.erp_px_cat_g1v2';
		BULK INSERT bronze.erp_px_cat_g1v2
		from 'C:\Users\lovel\Downloads\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_erp\PX_CAT_G1V2.csv'
		with(
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT '>>LOAD Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' SECONDS';
		
		SET @batch_end_time = GETDATE();
		PRINT'======================================='
		PRINT 'LOADING Bronze layer is completed';
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
End
----------------------------------------------------------------------Bulk insert into Bronze Completed-----------------------------
