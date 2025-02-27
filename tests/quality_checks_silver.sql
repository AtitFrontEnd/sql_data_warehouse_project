/*
This file consist of the data tranformation and quality checks for the silver layer

This scripts performs various quality checks for data consistency, accuracy, and standardization across the 'Silver' layer.
It include the checks for 
- Null or duplicate primary keys.
- Unwanted spaces in string fields.
- Data Standardization and Consistency.
- Invalid date ranges and orders.
- Data consistency between related fields.

Usage Notes:
- Run these checks after data  loading Silver layer
- Invertigate and resolve and discrepancies found during the checks.*/


------Transformation for Table 1 customer info

----Removing the duplicate values from primary key
select cst_id, count(*) from bronze.crm_cust_info 
group by cst_id
having count(*) > 1 or cst_id is NULL
 
 ----Flag_last is an example of data filtering where latest values are taken if we have 2 or more entries
SELECT * FROM (
select *, ROW_NUMBER() OVER (PARTITION BY CST_ID ORDER BY CST_CREATE_DATE DESC) AS FLAG_LAST 
from bronze.crm_cust_info)t
WHERE FLAG_LAST = 1 


---Check for unwated spaces first_name
--Exceptation: No results
SELECT cst_firstname from bronze.crm_cust_info
where cst_firstname != TRIM(cst_firstname) 

---Check for unwated spaces lastname
--Exceptation: No results
SELECT cst_lastname from bronze.crm_cust_info
where cst_lastname != TRIM(cst_lastname) 

---Data standardization & Consistency
----for example 'f' tp Female 
select 
cst_id,
cst_key,
TRIM(cst_firstname) AS cst_firstname,
TRIM(cst_lastname) AS cst_lastname,

CASE WHEN UPPER(TRIM(cst_material_status)) = 'S' THEN 'Single'
	WHEN UPPER(TRIM(cst_material_status)) = 'M' THEN 'Married'
	ELSE 'n/a'
END cst_material_status,

CASE WHEN UPPER(TRIM(cst_gndr)) = 'F' THEN 'Female'
	WHEN UPPER(TRIM(cst_gndr)) = 'M' THEN 'Male'
	ELSE 'n/a'
END cst_gndr,
cst_create_date
from bronze.crm_cust_info

select * from silver.crm_cust_info;

-------------------------Transformation for Table 2------------------
---Chk for unwanted spaces for name
select prd_nm from bronze.crm_prod_info
where prd_nm != TRIM(prd_nm) 

---CHK for null or negatives number (Colummn: Cost)
---Expectation: No results
select prd_cost, prd_id from bronze.crm_prod_info
where prd_cost < 0 or prd_cost IS NULL 

---Chk for prd_line
select distinct prd_line 
from bronze.crm_prod_info

----Check for invalid date
select * from bronze.crm_prod_info
where prd_end_dt < prd_start_dt


----------------------------------Transformation for table 3------------

where sls_order_dt = 0 

 select * from silver.crm_sales_details
 ---Chk for invalid dates, if it is 0 in sls date, put it as NULL
 select 
 NULLIF(sls_order_dt, 0) sls_order_dt
 from silver.crm_sales_details
  where sls_order_dt <= 0 
  OR LEN(sls_order_dt) != 8
  OR sls_order_dt > 20500101
  OR sls_order_dt < 19000101

 ---We cannot convert the int to date in sql, so we need to convert 'CAST' it to VARCHAR first. then caste it to date.
 --Checking that prod key in sales are also simlarly present in Product table 2. 
 where sls_prd_key NOT IN (select prd_key FROM silver.crm_prod_info)

 ---Chk for unwanted space from ord number column
where sls_ord_num != TRIM(sls_ord_num)

----Check data consistency: Between Sales, Quantity and Price
----sales = Quantity * Price
----Values must not be NULL, zero or negative

SELECT DISTINCT
sls_sales,
sls_quantity,
sls_price
FROM bronze.crm_sales_details
WHERE sls_sales != sls_quantity * sls_price
OR sls_sales IS NULL OR sls_quantity IS NULL OR sls_price IS NULL
OR sls_sales <= 0 OR sls_quantity <= 0 OR sls_price <= 0
 ORDER BY sls_sales, sls_quantity, sls_price

 ------Business Rules to change the bad data from source
 ---If sales is negative, zero, or null derive it using Quantity and Price.
 -----Price is zero or null, calculate it using Sales and Quantity 
 ------If price is negative, convert it to a positivve value.

 SELECT DISTINCT 
 sls_sales AS old_sls_sales,
 sls_quantity, 
 sls_price AS old_sls_price,
CASE WHEN sls_sales IS NULL OR sls_sales <= 0 OR sls_sales != sls_quantity * ABS(sls_price)
	THEN sls_quantity * ABS(sls_price)
	ELSE sls_sales
END AS sls_sales,

CASE WHEN sls_price IS NULL OR sls_price <= 0
	THEN sls_sales / NULLIF (sls_quantity, 0)
	ELSE sls_price 
END  AS sls_price
from bronze.crm_sales_details

------After loading the data to silver 
select * from silver.crm_sales_details
where sls_order_dt > sls_ship_dt or sls_order_dt > sls_due_dt

-----------------------Transformation 5---erp a101--------------------
---Data standardization and consistency
SELECT DISTINCT  cntry
from bronze.erp_loc_a101
ORDER BY cntry

---------------Transformation for table 6 erp g1v2-----

select * from silver.erp_px_cat_g1v2
---check for unwanted spaces in the columns
select * from bronze.erp_px_cat_g1v2
where maintenance != TRIM(maintenance) OR subcat != TRIM(subcat)

----Check for data standardization
select DISTINCT 
maintenance
FROM bronze.erp_px_cat_g1v2

--NICE DATA QUALITY FOR THE ABOVE TABLE SSO NO TRANSFORM

