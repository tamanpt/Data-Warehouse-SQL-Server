--===Create Gold Views===--
/*
This script creates view for gold layer in datawarehouse
Gold layer represent star schema that use fact and dimension tables

READY to use data (clean, enriched, business-ready)
*/
--==Create dimensions for customers==--
IF OBJECT_ID('gold.dim_customers', 'V') IS NOT NULL
	DROP VIEW gold.dim_customers;
GO

CREATE VIEW gold.dim_customers AS 
SELECT 
	  row_number()over(order by cst_id) as surrogate_customer_key
	  ,ci.[cst_id] AS customer_id
      ,ci.[cst_key]	AS customer_number
      ,ci.[cst_firstname] AS first_name
      ,ci.[cst_lastname] AS last_name
	  ,la.cntry AS country
      ,ci.[cst_material_status] AS marital_status 
      ,CASE WHEN ci.cst_gender != '_NA' THEN ci.cst_gender
			ELSE COALESCE(ca.gen, '_NA')
		END as gender
	  ,ca.bdate AS birthdate
      ,ci.[cst_create_date] AS create_date
	  ,ca.gen
  FROM [DataWarehouse].[silver].[crm_cust_info] ci
  LEFT JOIN silver.erp_cust_az12 ca ON ci.cst_key = ca.cid
  LEFT JOIN silver.erp_loc_a101 la ON ci.cst_key = la.cid;

  GO
  
--==Create dimensions for gold.dim_products ==--
IF OBJECT_ID('gold.dim_products ', 'V') IS NOT NULL
	DROP VIEW gold.dim_products;
GO

CREATE VIEW gold.dim_products AS
SELECT 
	  ROW_NUMBER()OVER(ORDER BY pn.prd_start_dt, pn.prd_key) AS surrogate_product_key
	  ,pn.[prd_id] AS product_id
      ,pn.[prd_key] AS product_number
	  ,pn.[prd_nm] AS product_name
	  ,pn.[cat_id] AS category_id
	  ,pc.cat AS category
	  ,pc.subcat AS subcategory
	  ,pc.maintenance
      ,pn.[prd_cost] AS cost
      ,pn.[prd_line] AS product_line
      ,pn.[prd_start_dt] AS [start_date]
  FROM [DataWarehouse].[silver].[crm_prd_info] pn
  LEFT JOIN silver.erp_px_cat_g1v2 pc ON pn.cat_id = pc.id
  WHERE prd_end_dt IS NULL;

GO

--==Create dimensions for gold.dim_products ==--
IF OBJECT_ID('gold.fact_sales ', 'V') IS NOT NULL
	DROP VIEW gold.fact_sales;
GO

CREATE VIEW gold.fact_sales AS
SELECT [sls_ord_num] AS order_number
	  ,pr.surrogate_product_key
	  ,cu.surrogate_customer_key
      ,[sls_order_dt] AS order_date
      ,[sls_ship_dt] AS shipping_date
      ,[sls_due_dt] AS due_date
      ,[sls_sales] AS sales_amount
      ,[sls_quantity] AS quantity
      ,[sls_price] AS price
  FROM [DataWarehouse].[silver].[crm_sales_details] sd
  LEFT JOIN gold.dim_customers cu ON sd.sls_cust_id = cu.customer_id
  LEFT JOIN gold.dim_products pr ON sd.sls_prd_key = pr.product_number;
