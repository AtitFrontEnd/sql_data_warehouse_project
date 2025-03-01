/*
======================================
DDL Script: Create Gold Views
====================================
Script Purpose:
  This script creates view for the gold layer in the data warehouse.
  This Gold layer represent the final dimension and fact tables (star Schema)

Each view performs transformation and combines data from the silver layer to produce a clean, enriched and business ready dataset.

Usage: 
-The views can be queried directly for analytics and reporting.
=============================================================

*/


CREATE VIEW gold.dim_customers AS  
SELECT 
	ROW_NUMBER() OVER (ORDER BY cst_id) AS customer_key,
	ci.cst_id AS customer_id,
	ci.cst_key AS customer_number, 
	ci.cst_firstname AS first_name,
	ci.cst_lastname AS marital_status,
	la.cntry AS country,
	ci.cst_material_status,
	CASE WHEN ci.cst_gndr != 'n/a' THEN ci.cst_gndr ---CRM is the master table
		ELSE COALESCE(ca.gen, 'n/a')
	END AS gender,
	ci.cst_create_date AS create_date,
	ca.bdate AS birthdate
FROM silver.crm_cust_info ci
LEFT JOIN silver.erp_cust_az12 ca
ON ci.cst_key = ca.cid
LEFT JOIN silver.erp_loc_a101 la
ON ci.cst_key = la.cid




---------------------Checking for gender column there are nulls---
SELECT distinct
	ci.cst_gndr,
	ca.gen,
	CASE WHEN ci.cst_gndr != 'n/a' THEN ci.cst_gndr ---CRM is the master table
		ELSE COALESCE(ca.gen, 'n/a')
	END AS new_gen
FROM silver.crm_cust_info ci
LEFT JOIN silver.erp_cust_az12 ca
ON ci.cst_key = ca.cid
LEFT JOIN silver.erp_loc_a101 la
ON ci.cst_key = la.cid
order by 1,2

---*******************---Creating a product tables for gold----*********************-------
CREATE VIEW gold.dim_product AS
SELECT 
ROW_NUMBER() over (ORDER BY pn.prd_start_dt, pn.prd_key) AS product_key,
pn.prd_id AS product_id,
pn.prd_key AS product_number,
pn.prd_nm AS product_name,
pn.cat_id AS category_id,
pc.cat AS category,
pc.subcat AS subcategory,
pc.maintenance,
pn.prd_cost AS cost,
pn.prd_line AS product_line,
pn.prd_start_dt,
pn.prd_end_dt
FROM 
silver.crm_prod_info pn
LEFT JOIN silver.erp_px_cat_g1v2 pc
ON pn.cat_id = pc.id
WHERE prd_end_dt IS NULL

select * from silver.crm_prod_info


-----Checking for duplicates after intigrating the tables---------
select prd_key, COUNT(*) FROM (
SELECT 
pn.prd_id,
pn.cat_id,
pn.prd_key,
pn.prd_nm,
pn.prd_cost,
pn.prd_line,
pn.prd_start_dt,
pn.prd_end_dt,
pc.cat,
pc.maintenance
FROM 
silver.crm_prod_info pn
LEFT JOIN silver.erp_px_cat_g1v2 pc
ON pn.cat_id = pc.id
WHERE prd_end_dt IS NULL
)t GROUP BY prd_key
having count(*) > 1

------------------------------------Creating FACT Table------------------------
create view gold.fact_sales AS
SELECT
sd.sls_ord_num AS order_number,
pr.product_key,
cu.customer_key,
sd.sls_prd_key AS order_date,
sd.sls_cust_id AS shipping_date,
sd.sls_order_dt AS due_date,
sd.sls_ship_dt AS sales_amount,
sd.sls_due_dt AS quantity,
sd.sls_sales AS price,
sd.sls_quantity,
sd.sls_price 
FROM
silver.crm_sales_details sd
LEFT JOIN gold.dim_product pr
ON sd.sls_prd_key = pr.product_number
left join gold.dim_customers cu
ON sd.sls_cust_id = cu.customer_id

----Joining the fact table with dim tables


select * from gold.fact_sales f
left join gold.dim_customers c
on c.customer_key = f.customer_key
left join gold.dim_product p
on p.product_key = f.customer_key
where c.customer_key is NULL
