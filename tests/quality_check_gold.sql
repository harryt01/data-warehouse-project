SELECT cst_id, COUNT(*) FROM
(
SELECT
	ci.cst_id,
	ci.cst_key,
	ci.cst_firstname,
	ci.cst_lastname,
	ci.cst_marital_status,
	ci.cst_gender,
	ci.cst_create_date,
	ca.bdate,
	ca.gen,
	la.cntry
FROM silver.crm_cust_info ci
LEFT JOIN silver.erp_cust_az12 ca
ON ci.cst_key = ca.cid
LEFT JOIN silver.erp_loc_a101 la
ON ci.cst_key = la.cid
)t
GROUP BY cst_id
HAVING COUNT(*) > 1;


--check gender values for data validation
SELECT DISTINCT
	--ci.cst_id,
	--ci.cst_key,
	--ci.cst_firstname,
	--ci.cst_lastname,
	--ci.cst_marital_status,
	CASE 
		WHEN ci.cst_gender != 'n/a' THEN ci.cst_gender --CRM is the master for gender info
		ELSE COALESCE(ca.gen, 'n/a')
	END cst_gender,
	
	--ci.cst_create_date,
	--ca.bdate,
	--ca.gen,
	--la.cntry
	ca.gen
FROM silver.crm_cust_info ci
LEFT JOIN silver.erp_cust_az12 ca
ON ci.cst_key = ca.cid
LEFT JOIN silver.erp_loc_a101 la
ON ci.cst_key = la.cid
ORDER BY 1, 2;

--main load query for gold.dim_cust_info
CREATE VIEW gold.dim_customers AS
SELECT
	ROW_NUMBER() OVER (ORDER BY cst_id) AS customer_key,
	ci.cst_id AS customer_id,
	ci.cst_key AS customer_number,
	ci.cst_firstname AS first_name,
	ci.cst_lastname AS last_name,
	la.cntry AS country,
	ci.cst_marital_status AS marital_status,
	CASE 
		WHEN ci.cst_gender != 'n/a' THEN ci.cst_gender --CRM is the master for gender info
		ELSE COALESCE(ca.gen, 'n/a')
	END gender,
	ca.bdate AS birthdate,
	ci.cst_create_date AS create_date
FROM silver.crm_cust_info ci
LEFT JOIN silver.erp_cust_az12 ca
ON ci.cst_key = ca.cid
LEFT JOIN silver.erp_loc_a101 la
ON ci.cst_key = la.cid;


SELECT * FROM gold.dim_customers;


--gold dimension for product info
SELECT prd_key, COUNT(*) FROM (
SELECT 
	pr.prd_id,
	pr.prd_key,
	pr.cat_id,
	pr.prd_nm,
	pr.prd_cost,
	pr.prd_line,
	pr.prd_start_dt,
	--pr.prd_end_dt,
	pc.cat,
	pc.subcat,
	pc.maintenance
FROM silver.crm_prd_info pr
LEFT JOIN silver.erp_px_cat_g1v2 pc
ON pr.cat_id = pc.id
WHERE prd_end_dt IS NULL	--filter out historical data, only load current data
) t
GROUP BY prd_key
HAVING COUNT(*) > 1;

SELECT * FROM gold.dim_products;

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
ON sd.sls_cust_id = cu.customer_id


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