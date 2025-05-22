/*
==========
Quality checks
==========
Script Purpose:
    Performs quality checks for data integrity, consistency, accuracy, 
    and standardization across the Gold layer, including:
    - Uniqueness of surrogate keys in dimension tables.
    - Referential integrity between fact and dimension tables.
    - Validation of relationships in the data model for analytical purposes.

==========
*/

--check for uniqueness of customer key in gold.dim_customers
SELECT 
    customer_key,
    COUNT(*) AS duplicate_count
FROM gold.dim_customers
GROUP BY customer_key
HAVING COUNT(*) > 1;


--check gender values for data validation
SELECT DISTINCT
	CASE 
		WHEN ci.cst_gender != 'n/a' THEN ci.cst_gender --CRM is the master for gender info
		ELSE COALESCE(ca.gen, 'n/a')
	END cst_gender,
	ca.gen
FROM silver.crm_cust_info ci
LEFT JOIN silver.erp_cust_az12 ca
ON ci.cst_key = ca.cid
LEFT JOIN silver.erp_loc_a101 la
ON ci.cst_key = la.cid
ORDER BY 1, 2;


--check for uniqueness of customer key in gold.dim_products
SELECT 
    product_key,
    COUNT(*) AS duplicate_count
FROM gold.dim_products
GROUP BY product_key
HAVING COUNT(*) > 1;

SELECT * FROM gold.dim_products;

/*
--joining facts from crm_sales_details with dimensions using dimension's surrogate keys
SELECT
	sd.sls_ord_num AS order_number,
	pr.product_key,
	cu.customer_key,
	sd.sls_order_dt AS order_date,
	sd.sls_ship_dt AS shipping_date,
	sd.sls_due_dt AS due_date,
	sd.sls_sales AS sales_amount,
	sd.sls_quantity AS quantity,
	sd.sls_price AS price
FROM silver.crm_sales_details sd
LEFT JOIN gold.dim_products pr
ON sd.sls_prd_key = pr.product_number
LEFT JOIN gold.dim_customers cu
ON sd.sls_cust_id = cu.customer_id;
*/


--fact check: check if all dimension tables can successfully join the fact table
--foreign key integrity	(dimensions):
SELECT * FROM gold.fact_sales f
LEFT JOIN gold.dim_customers c
ON f.customer_key = c.customer_key
WHERE c.customer_key IS NULL;

--
SELECT * FROM gold.fact_sales f
LEFT JOIN gold.dim_products p
ON f.product_key = p.product_key
WHERE p.product_key IS NULL;

SELECT * FROM gold.fact_sales;