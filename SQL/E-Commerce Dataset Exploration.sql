-- Query 01: calculate total visit, pageview, transaction for Jan, Feb and March 2017 (order by month)
SELECT distinct format_date('%Y%m',PARSE_DATE('%Y%m%d', date))  as month,
sum(totals.visits) as visits,
sum(totals.pageviews) as pageviews,
sum(totals.transactions) as transactions
FROM `bigquery-public-data.google_analytics_sample.ga_sessions_2017*`
where _table_suffix between '0101' and '0331'
group by month
order by month
-- Query 02: Bounce rate per traffic source in July 2017 (Bounce_rate = num_bounce/total_visit) (order by total_visit DESC)
SELECT  distinct trafficSource.source,
sum(totals.visits) as total_visits,
sum(totals.bounces) as total_no_of_bounces,
round(sum(totals.bounces)*100/sum(totals.visits),8) as bounce_rate
FROM `bigquery-public-data.google_analytics_sample.ga_sessions_201707*`
group by trafficSource.source
order by total_visits desc
-- Query 3: Revenue by traffic source by week, by month in June 2017
SELECT * FROM
(SELECT 'month' as time_type,
 format_date('%Y%m',PARSE_DATE('%Y%m%d', date))  as time,
      trafficSource.source as source,
      round(sum(product.productRevenue/1000000),4) as revenue
FROM `bigquery-public-data.google_analytics_sample.ga_sessions_201706*`,
UNNEST (hits) hits,
UNNEST (hits.product) product
group by time ,trafficSource.source
union all
SELECT 'week' as time_type,
      format_date('%Y%W',PARSE_DATE('%Y%m%d', date))  as time,
      trafficSource.source as source,
      round(sum(product.productRevenue/1000000),4) as revenue     
FROM `bigquery-public-data.google_analytics_sample.ga_sessions_201706*`,
UNNEST (hits) hits,
UNNEST (hits.product) product
group by time ,trafficSource.source)
order by revenue desc
-- Query 04: Average number of pageviews by purchaser type (purchasers vs non-purchasers) in June, July 2017.
WITH nonpurchase as (
    SELECT
        FORMAT_DATE("%Y%m",PARSE_DATE('%Y%m%d',date)) as month,
        ROUND(sum(totals.pageviews)/count(distinct fullVisitorId),8) as avg_pageviews_nonpurchase
    FROM `bigquery-public-data.google_analytics_sample.ga_sessions_*` ,
      UNNEST (hits) hits,
      UNNEST (hits.product) product
    Where 
        _table_suffix between '20170601' and '20170731'
        AND totals.transactions is null  and product.productRevenue is null 
    GROUP BY month
)
, purchase as (
    SELECT
        FORMAT_DATE("%Y%m",PARSE_DATE('%Y%m%d',date)) as month,
        ROUND(sum(totals.pageviews)/count(distinct fullVisitorId),8) as avg_pageviews_purchase
    FROM `bigquery-public-data.google_analytics_sample.ga_sessions_*`,
      UNNEST (hits) hits,
      UNNEST (hits.product) product
    Where 
        _table_suffix between '20170601' and '20170731'
        AND totals.transactions >= 1 and productRevenue is not null
    GROUP BY month
)

SELECT
    nonpurchase.month,
    purchase.avg_pageviews_purchase,
    nonpurchase.avg_pageviews_nonpurchase
FROM nonpurchase
inner join  purchase USING(month)
-- Query 05: Average number of transactions per user that made a purchase in July 2017
 SELECT
        FORMAT_DATE("%Y%m",PARSE_DATE('%Y%m%d',date)) as month,
        Round(sum(totals.transactions)/count(distinct fullVisitorId),8) as Avg_total_transactions_per_user
    FROM `bigquery-public-data.google_analytics_sample.ga_sessions_201707*` ,
      UNNEST (hits) hits,
      UNNEST (hits.product) product
    Where 
        totals.transactions >= 1 and product.productRevenue is not null 
    GROUP BY month
-- Query 06: Average amount of money spent per session. Only include purchaser data in July 2017
 SELECT
        FORMAT_DATE("%Y%m",PARSE_DATE('%Y%m%d',date)) as month,
        sum(product.productRevenue)/(count(totals.visits)*1000000) as Avg_total_transactions_per_user
    FROM `bigquery-public-data.google_analytics_sample.ga_sessions_201707*` ,
      UNNEST (hits) hits,
      UNNEST (hits.product) product
    Where 
        totals.transactions is not null  and product.productRevenue is not null 
    GROUP BY month
-- Query 07: Other products purchased by customers who purchased product "YouTube Men's Vintage Henley" in July 2017. Output should show product name and the quantity was ordered.
WITH product as (
    SELECT
        fullVisitorId,
        product.v2ProductName,
        product.productRevenue,
        product.productQuantity 
    FROM `bigquery-public-data.google_analytics_sample.ga_sessions_201707*`,
        UNNEST(hits) as hits,
        UNNEST(hits.product) as product
    Where 
        product.productRevenue IS NOT NULL
)

SELECT
    product.v2ProductName as other_purchased_products,
    SUM(product.productQuantity) as quantity
FROM product
WHERE 
    product.fullVisitorId IN (
        SELECT fullVisitorId
        FROM product
        WHERE product.v2ProductName LIKE "YouTube Men's Vintage Henley"

    )
    AND product.v2ProductName NOT LIKE "YouTube Men's Vintage Henley"
GROUP BY other_purchased_products
ORDER BY quantity desc
-- Query 08:
WITH product_view as (
    SELECT
        FORMAT_DATE("%Y%m",PARSE_DATE('%Y%m%d',date)) as month,
        COUNT(product.productSKU) as num_product_view
    FROM `bigquery-public-data.google_analytics_sample.ga_sessions_*`,
        UNNEST(hits) as hits,
        UNNEST(product) as product
    WHERE 
        _table_suffix between '20170101' and '20170331'
        AND eCommerceAction.action_type = '2'
    GROUP BY month
)

, addtocart as (
    SELECT
        FORMAT_DATE("%Y%m",PARSE_DATE('%Y%m%d',date)) as month,
        COUNT(product.productSKU) as num_addtocart
    FROM `bigquery-public-data.google_analytics_sample.ga_sessions_*`,
        UNNEST(hits) as hits,
        UNNEST(product) as product
    WHERE 
        _table_suffix between '20170101' and '20170331'
        AND eCommerceAction.action_type = '3'
    GROUP BY month
)

, purchase as (
    SELECT
        FORMAT_DATE("%Y%m",PARSE_DATE('%Y%m%d',date)) as month,
        COUNT(product.productSKU) as num_purchase
    FROM `bigquery-public-data.google_analytics_sample.ga_sessions_*`,
        UNNEST(hits) as hits,
        UNNEST(product) as product
    WHERE 
        _table_suffix between '20170101' and '20170331'
        AND eCommerceAction.action_type = '6' and product.productRevenue is not null
    GROUP BY month
)

SELECT
    product_view.month,
    product_view.num_product_view,
    addtocart.num_addtocart,
    purchase.num_purchase,
    ROUND((num_addtocart/num_product_view)*100,2) as add_to_cart_rate,
    ROUND((num_purchase/num_product_view)*100,2) as purchase_rate
FROM product_view
JOIN addtocart USING(month)
JOIN purchase USING(month)
ORDER BY month