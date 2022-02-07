--Average discount for each category
SELECT category, CAST(ROUND(AVG(discount_no_null), 2) AS DECIMAL(18,2)) AS average_discount
FROM glue_schema_zipmex.order_detail_new AS a
INNER JOIN glue_schema_zipmex.restaurant_detail_new AS b
ON a.restaurant_id = b.id
GROUP BY category;


--Row count per each cooking_bin
SELECT cooking_bin, COUNT(cooking_bin) AS count_cookging_bin 
FROM glue_schema_zipmex.restaurant_detail_new
GROUP BY cooking_bin