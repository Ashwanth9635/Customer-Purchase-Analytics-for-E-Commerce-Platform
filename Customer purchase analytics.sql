create database customer_purchase;




-- A. Basic Aggregation:
-- Find the total number of purchases per product per year  
SELECT 
    YEAR(STR_TO_DATE(purchase_date, '%d-%m-%Y %H:%i')) AS purchase_year,
    pr.product_name,
    pr.category,
    COUNT(p.quantity) AS total_purchases
FROM Purchase p
JOIN Products pr ON p.product_id = pr.product_id
GROUP BY purchase_year,  pr.product_name, pr.category
ORDER BY purchase_year DESC, total_purchases DESC;
-- INSIGHTS: It helps us to manage inventory






-- describe purchase;

-- select * from purchase;

-- SELECT DISTINCT purchase_date FROM Purchase ORDER BY purchase_date;

-- FROM Purchase 
-- ORDER BY available_years;

-- SELECT purchase_date, STR_TO_DATE(purchase_date, '%d-%m-%Y %H:%i') AS converted_date
-- FROM Purchase
-- ORDER BY purchase_date
-- LIMIT 20;

-- ALTER TABLE Purchase MODIFY COLUMN purchase_date DATETIME;
-- UPDATE Purchase 
-- SET purchase_date = STR_TO_DATE(purchase_date, '%d-%m-%Y %H:%i');

SET SQL_SAFE_UPDATES = 0;

-- SELECT DISTINCT 
--     YEAR(purchase_date, '%d-%m-%Y %H:%i') AS extracted_year 
-- FROM Purchase 
-- ORDER BY extracted_year;

-- SELECT 
--     YEAR(STR_TO_DATE(purchase_date, '%d-%m-%Y %H:%i')) AS purchase_year,
--     COUNT(*) AS total_rows
-- FROM Purchase
-- GROUP BY purchase_year
-- ORDER BY purchase_year DESC;

-- SELECT 
--     YEAR(p.purchase_date_only) AS purchase_year,
--     p.product_id,
--     pr.category,
--     pr.product_name,
--     SUM(p.product_id) AS total_purchases
-- FROM Purchase p
-- JOIN Products pr ON p.product_id = pr.product_id
-- GROUP BY purchase_year, p.product_id, pr.product_name, pr.category
-- ORDER BY purchase_year ASC, total_purchases DESC
-- LIMIT 100000;

-- select * from products;

-- UPDATE purchase
-- SET purchase_date = CAST(purchase_date AS DATE);

-- ALTER TABLE purchase
-- ADD COLUMN purchase_date_only DATE,
-- ADD COLUMN purchase_time_only TIME;

-- UPDATE purchase
-- SET
--     purchase_date_only = DATE(purchase_date),
--     purchase_time_only = TIME(purchase_date);

-- DESCRIBE purchase;	

UPDATE purchase
SET purchase_date = STR_TO_DATE(purchase_date, '%d-%m-%Y %H:%i')
WHERE purchase_date IS NOT NULL;

-- select * from purchase;






-- list down the top selling products every year 
WITH RankedProducts AS (
    SELECT 
        YEAR(STR_TO_DATE(p.purchase_date, '%d-%m-%Y %H:%i')) AS purchase_year,
        pr.product_name,
        pr.category,
        SUM(p.quantity) AS total_purchases,
        RANK() OVER (PARTITION BY YEAR(STR_TO_DATE(p.purchase_date, '%d-%m-%Y %H:%i')) ORDER BY SUM(p.quantity) DESC) AS rank_position
    FROM Purchase p
    JOIN Products pr ON p.product_id = pr.product_id
    GROUP BY purchase_year,  pr.product_name,pr.category
)
SELECT * FROM RankedProducts WHERE rank_position = 1;
-- Insights: Changing product trends indicate customer preferences shift over time,businesses should adjust inventory planning.








-- Calculate the average quantity purchased per product and list down the top 5 products with high avg quantity purchased
select 
     p.product_id, pr.product_name, pr.category,AVG(p.quantity) 
from purchase p join products pr on p.product_id=pr.product_id 
group by p.product_id, pr.product_name, pr.category 
order by avg(p.quantity) desc
LIMIT 5;
-- Insights: The query results provide key business insights about purchasing patterns and product demand. Introduce bulk pricing discounts to attract more customers.







-- B. Join Operations:
-- Join purchase history with products dataset to get the product name for each purchase.
SELECT 
    p.purchase_id,
    p.product_id,
    pr.product_name,
    pr.category,
    pr.price_per_unit,
    pr.brand,
    pr.product_description,
    pr.category,
    p.purchase_date,
    p.quantity,
    p.total_amount
FROM Products pr
JOIN Purchase p ON pr.product_id = p.product_id;
-- Insights: It tells us which product has been purchased on which date and total amount contributed on that particular day






-- Join purchase history with customer profile dataset to include customer information for each purchase and list top 5 customers with high purchases
SELECT 
    c.customer_id,
    CONCAT(c.first_name, ' ', c.last_name) AS customer_name,
    c.email,
    c.city,
    c.state,
    COUNT(p.purchase_id) AS total_purchases,  -- Number of purchases made
    SUM(p.total_amount) AS total_spent        -- Total money spent
FROM Purchase p
JOIN Customer c ON p.customer_id = c.customer_id
GROUP BY c.customer_id, customer_name, c.email, c.city, c.state
ORDER BY total_spent DESC
LIMIT 5;
-- Insights: This shows the valueable customer who has contributed more to the business and helps to give extra discount to retain the client








-- C. Window Functions:
-- Find the cumulative sum of purchases for each product category over year.
WITH YearlyCategorySales AS (
    SELECT 
        YEAR(STR_TO_DATE(purchase_date, '%d-%m-%Y %H:%i')) AS purchase_year,
        pr.category,
        SUM(p.quantity) AS total_purchases
    FROM Purchase p
    JOIN Products pr ON p.product_id = pr.product_id
    GROUP BY purchase_year, pr.category
)
SELECT 
    purchase_year,
    category,
    total_purchases,
    SUM(total_purchases) OVER (PARTITION BY category ORDER BY purchase_year) AS cumulative_purchases
FROM YearlyCategorySales
ORDER BY category, purchase_year;
-- Insights: This shows that every product category sales has increased each year






-- D. Rank and Dense Rank:
-- Rank customers based on their total expenditure.
SELECT 
    c.customer_id,
    CONCAT(c.first_name, ' ', c.last_name) AS customer_name,
    c.email,
    c.city,
    c.state,
    SUM(p.total_amount) AS total_spent,
    RANK() OVER (ORDER BY SUM(p.total_amount) DESC) AS spending_rank
FROM Purchase p
JOIN Customer c ON p.customer_id = c.customer_id
GROUP BY c.customer_id, customer_name, c.email, c.city, c.state
ORDER BY spending_rank;
-- Insights: Using rank command we could rank the client based on their total purchase amount






-- Identify the top 10 customers by purchase frequency using DENSE_RANK().
SELECT 
    c.customer_id,
    CONCAT(c.first_name, ' ', c.last_name) AS customer_name,
    c.email,
    c.city,
    c.state,
    COUNT(p.purchase_id) AS total_purchases,  
    DENSE_RANK() OVER (ORDER BY COUNT(p.purchase_id) DESC) AS rank_position
FROM Purchase p
JOIN Customer c ON p.customer_id = c.customer_id
GROUP BY c.customer_id, customer_name, c.email, c.city, c.state
ORDER BY rank_position
LIMIT 10;
-- Insights: Dense rank command helps us to rank the frequency of visit by client in same rank in case the no. of purchase is same








-- SELECT 
--     c.city,  -- Replace with actual segment column (e.g., customer type, age group)
--     PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY total_spent) AS percentile_25,
--     PERCENTILE_CONT(0.50) WITHIN GROUP (ORDER BY total_spent) AS percentile_50, -- Median
--     PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY total_spent) AS percentile_75
-- FROM (
--     SELECT 
--         c.customer_id,
--         c.city,  -- Replace with actual segment (e.g., city, membership_level, etc.)
--         SUM(p.total_amount) AS total_spent
--     FROM Purchase p
--     JOIN Customers c ON p.customer_id = c.customer_id
--     GROUP BY c.customer_id, c.customer_segment
-- ) customer_purchases
-- GROUP BY c.city;










-- E. Percentiles:
-- Calculate the 25th, 50th (median), and 75th percentiles of total purchase amounts for each customer segment.
WITH customer_purchases AS (
    SELECT 
        c.customer_id,
        c.city,  
        SUM(p.total_amount) AS total_purchase
    FROM Purchase p
    JOIN Customer c ON p.customer_id = c.customer_id
    GROUP BY c.customer_id, c.city
),
ranked_customers AS (
    SELECT 
        city,
        total_purchase,
        NTILE(100) OVER (PARTITION BY city ORDER BY total_purchase) AS percentile_rank
    FROM customer_purchases
)
SELECT 
    city,
    ROUND(MAX(CASE WHEN percentile_rank = 25 THEN total_purchase END),0) AS percentile_25,
    ROUND(MAX(CASE WHEN percentile_rank = 50 THEN total_purchase END),0) AS percentile_50, -- Median
    ROUND(MAX(CASE WHEN percentile_rank = 75 THEN total_purchase END),0) AS percentile_75
FROM ranked_customers
GROUP BY city;
-- Insights: This shows that city pheonix min purchase is itself more than the other city






 
-- select pr.product_id, pr.category, median(p.purchase_amount) as median_amount, count(p.purchase_amount)  
-- from products pr join purchase p on pr.product_id = p.product_id 
-- group by pr.category 
-- order by  pr.product_id, median_amount desc, count(p.purchase_amount) desc;








-- F. Median Calculation:
-- Compute the median purchase amount per product category.
WITH purchase_ranks AS (
    SELECT 
        pr.category,
        p.total_amount,
        ROW_NUMBER() OVER (PARTITION BY pr.category ORDER BY p.total_amount) AS row_num,
        COUNT(*) OVER (PARTITION BY pr.category) AS total_rows,
        SUM(p.total_amount) OVER (PARTITION BY pr.category) AS total_sales
    FROM Purchase p
    JOIN Products pr ON p.product_id = pr.product_id
),
median_values AS (
    SELECT 
        category,
        total_sales,
        total_amount AS median_value
    FROM purchase_ranks
    WHERE row_num = CEIL(total_rows / 2) -- Picks the middle value for odd count
       OR row_num = FLOOR(total_rows / 2) + 1 -- Picks second middle value for even count
)
SELECT 
    category,
    ROUND(AVG(median_value), 0) AS median_purchase_amount,
    ROUND(MAX(total_sales), 0) AS total_sales
FROM median_values
GROUP BY category;
-- Insights: The median is less affected by outliers. It provides a better representation of typical spending in each category.
-- Categories with a higher median purchase amount indicate that customers typically spend more on those products.
-- Lower median values suggest budget-friendly or frequently discounted items.






-- G. Complex Aggregation:
-- Find the average, maximum, and minimum purchase value for each product type 
SELECT 
    pr.category AS product_category,
    ROUND(AVG(p.total_amount), 2) AS avg_purchase_value,
    MAX(p.total_amount) AS max_purchase_value,
    MIN(p.total_amount) AS min_purchase_value
FROM Purchase p
JOIN Products pr ON p.product_id = pr.product_id
GROUP BY pr.category
ORDER BY avg_purchase_value DESC;
-- Insights: Meat has contributed the max purchase value to business. It shows the demand for the product and needs to maintain more inventory.
-- Other products need proper marketing and discounting to push further amount of sales





-- WITH customer_age AS (
--     SELECT 
--         c.customer_id,
--         TIMESTAMPDIFF(YEAR, c.date_of_birth, CURDATE()) AS age
--     FROM Customer c
-- ),
-- age_groups AS (
--     SELECT 
--         ca.customer_id,
--         CASE 
--             WHEN ca.age BETWEEN 25 AND 35 THEN '25-35'
--             WHEN ca.age BETWEEN 36 AND 45 THEN '36-45'
--             WHEN ca.age BETWEEN 46 AND 55 THEN '46-55'
--             WHEN ca.age BETWEEN 56 AND 65 THEN '56-65'
--             ELSE '70+'
--         END AS age_group
--     FROM customer_age ca
-- )
-- SELECT 
--     ag.age_group,
--     ROUND(AVG(p.total_amount), 2) AS avg_purchase_value,
--     MAX(p.total_amount) AS max_purchase_value,
--     MIN(p.total_amount) AS min_purchase_value
-- FROM Purchase p
-- JOIN age_groups ag ON p.customer_id = ag.customer_id
-- GROUP BY ag.age_group
-- ORDER BY FIELD(ag.age_group, '18-25', '26-35', '36-45', '46-60', '60+');








-- Find the average, maximum, and minimum purchase value for each customer age group.
WITH customer_age AS (
    SELECT 
    customer_id, 
    date_of_birth, 
    STR_TO_DATE(date_of_birth, '%d-%m-%Y %H:%i') AS converted_dob,
    TIMESTAMPDIFF(YEAR, STR_TO_DATE(date_of_birth, '%d-%m-%Y %H:%i'), CURDATE()) AS age
FROM Customer
),
age_groups AS (
    SELECT 
        ca.customer_id,
        CASE 
            WHEN ca.age  BETWEEN 25 AND 35 THEN '25-35'
            WHEN ca.age BETWEEN 36 AND 45 THEN '36-45'
            WHEN ca.age BETWEEN 46 AND 55 THEN '46-55'
            WHEN ca.age BETWEEN 56 AND 65 THEN '56-65'
            ELSE '70+'
        END AS age_group
    FROM customer_age ca
)
SELECT 
    ag.age_group,
    ROUND(AVG(p.total_amount), 0) AS avg_purchase_value,
    ROUND(MAX(p.total_amount),0) AS max_purchase_value,
    ROUND(MIN(p.total_amount),0) AS min_purchase_value
FROM Purchase p
JOIN age_groups ag ON p.customer_id = ag.customer_id
GROUP BY ag.age_group
order by ag.age_group asc;
-- Insights: 36-45 age group has purchased more. The avg value shows the frequency and qantity of purchase. Need to give disount to retain this set of clients.








-- SELECT 
--     customer_id, 
--     date_of_birth, 
--     TIMESTAMPDIFF(YEAR, date_of_birth, CURDATE()) AS age
-- FROM Customer
-- ORDER BY age;

-- SELECT customer_id, date_of_birth FROM Customer WHERE date_of_birth IS NULL;

-- describe customer;

-- SELECT 
--     customer_id, 
--     date_of_birth, 
--     STR_TO_DATE(date_of_birth, '%d-%m-%Y %H:%i') AS converted_dob,
--     TIMESTAMPDIFF(YEAR, STR_TO_DATE(date_of_birth, '%d-%m-%Y %H:%i'), CURDATE()) AS age
-- FROM Customer;


-- SELECT 
--     DAYNAME(STR_TO_DATE(purchase_date, '%d-%m-%Y %H:%i')) AS day_of_week,
--     COUNT(purchase_id) / COUNT(DISTINCT DATE(STR_TO_DATE(purchase_date, '%d-%m-%Y %H:%i'))) AS avg_purchases_per_day
-- FROM Purchase
-- GROUP BY day_of_week
-- ORDER BY FIELD(day_of_week, 'Sunday', 'Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday');

-- SELECT 
--     purchase_date,
--     STR_TO_DATE(purchase_date, '%d-%m-%Y %H:%i') AS converted_date
-- FROM Purchase
-- LIMIT 10;

-- SELECT DISTINCT purchase_date FROM Purchase;

-- SELECT 
--     DAYNAME(STR_TO_DATE(TRIM(purchase_date), '%d-%m-%Y %H:%i')) AS day_of_week,
--     COUNT(purchase_id) / COUNT(DISTINCT DATE(STR_TO_DATE(TRIM(purchase_date), '%d-%m-%Y %H:%i'))) AS avg_purchases_per_day
-- FROM Purchase
-- WHERE STR_TO_DATE(TRIM(purchase_date), '%d-%m-%Y %H:%i') IS NOT NULL
-- GROUP BY day_of_week
-- ORDER BY FIELD(day_of_week, 'Sunday', 'Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday');

-- SELECT COUNT(*) FROM Purchase WHERE STR_TO_DATE(purchase_date, '%d-%m-%Y %H:%i') IS NOT NULL;

-- ALTER TABLE Purchase MODIFY COLUMN purchase_date DATETIME;
-- UPDATE Purchase 
-- SET purchase_date = STR_TO_DATE(purchase_date, '%d-%m-%Y %H:%i')
-- WHERE STR_TO_DATE(purchase_date, '%d-%m-%Y %H:%i') IS NOT NULL;

-- SET SQL_SAFE_UPDATES = 0;

-- SELECT DISTINCT purchase_date 
-- FROM Purchase 
-- WHERE STR_TO_DATE(purchase_date, '%d-%m-%Y %H:%i') IS NULL;

-- SELECT 
--     purchase_date, 
--     STR_TO_DATE(purchase_date, '%d-%m-%Y %H:%i:%s') AS converted_date
-- FROM Purchase
-- LIMIT 10;








-- H. Grouping:
-- Group purchases by day of the week and find the average number of purchases made on each day.
SELECT 
    DAYNAME(STR_TO_DATE(purchase_date, '%d-%m-%Y %H:%i:%s')) AS day_of_week,
    COUNT(purchase_id) / COUNT(DISTINCT DATE(STR_TO_DATE(purchase_date, '%d-%m-%Y %H:%i:%s'))) AS avg_purchases_per_day
FROM Purchase
GROUP BY day_of_week
ORDER BY FIELD(day_of_week, 'Sunday', 'Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday');
-- Insights:  Mostly the client has bought more products in monday. Need to stock more and on other days sales is less. So need to give offers on any day of the week







-- Group customers by city and find the total number of purchases and total revenue generated.
SELECT 
    c.city, 
    COUNT(p.purchase_id) AS total_purchases, 
    ROUND(SUM(p.total_amount),0) AS total_revenue
FROM 
    Customer c
JOIN 
    Purchase p ON c.customer_id = p.customer_id
GROUP BY 
    c.city
Order By
    total_revenue Desc;
-- Insights: Phoenix has done high sales compared to other. Need to do more marketing campaigns on other cities for more visibility.







-- I. Case Statement
-- Classify customers as “High-Spending,” “Medium-Spending,” or “Low-Spending” based on their total purchase amounts (using percentiles in multiples of 33).
WITH CustomerSpending AS (
    SELECT 
        c.customer_id, 
        CONCAT(c.first_name, ' ', c.last_name) AS customer_name,    
        SUM(p.total_amount) AS total_spent
    FROM 
        Customer c
    JOIN 
        Purchase p ON c.customer_id = p.customer_id
    GROUP BY 
        c.customer_id, customer_name
),
RankedSpending AS (
    SELECT 
        customer_id,
        total_spent,
        NTILE(3) OVER (ORDER BY total_spent) AS spending_rank
    FROM 
        CustomerSpending
)
SELECT 
    cs.customer_id,
    cs.customer_name,
    cs.total_spent,
    CASE 
        WHEN rs.spending_rank = 1 THEN 'Low-Spending'
        WHEN rs.spending_rank = 2 THEN 'Medium-Spending'
        WHEN rs.spending_rank = 3 THEN 'High-Spending'
    END AS spending_category
FROM 
    CustomerSpending cs
JOIN 
    RankedSpending rs ON cs.customer_id = rs.customer_id
Order By
	Spending_category asc;
-- Insights:  This helps to know the valuable customers and offer discounts for those exceeding certain limit which might increase the sales of low and medium spending client 






-- J. Join with Condition:
-- Join purchase history with customer profile dataset where the customer’s age is above 30, and display their purchase details.
SELECT 
    c.customer_id,
    c.first_name,
    c.last_name,
    c.email,
    c.phone_number,
    TIMESTAMPDIFF(YEAR, STR_TO_DATE(c.date_of_birth, '%d-%m-%Y %H:%i'), CURDATE())  as age,
    p.purchase_id,
    p.product_id,
    p.purchase_date,
    p.quantity,
    p.total_amount
FROM 
    Customer c
JOIN 
    Purchase p ON c.customer_id = p.customer_id
WHERE 
    TIMESTAMPDIFF(YEAR, STR_TO_DATE(date_of_birth, '%d-%m-%Y %H:%i'), CURDATE()) > 30;
-- Insights: This helps the business to know the frequency of visit by clients which shows the loyalty





-- K. Top N Analysis:
-- Find the top 5 products contributing to the highest revenue.
SELECT 
    p.product_id,
    p.product_name,
    p.category,
    SUM(pur.total_amount) AS total_revenue
FROM 
    Purchase pur
JOIN 
    Products p ON pur.product_id = p.product_id
GROUP BY 
    p.product_id, p.product_name, p.category
ORDER BY 
    total_revenue DESC
LIMIT 5;
-- Insights: This helps the business to buy the product bulk with discount and priortize the product inventory






-- Identify the top 3 cities with the most number of unique customers.
SELECT 
    city,
    COUNT(DISTINCT customer_id) AS unique_customers
FROM 
    Customer
GROUP BY 
    city
ORDER BY 
    unique_customers DESC
LIMIT 3;
-- Insights: This shows more people visit the shop in pheonix. Other city needs to attract new clients by marketing with offers






-- L. Window Functions for Trend Analysis:
-- Create a 7-day moving average of total purchases per product 
WITH DailyPurchases AS (
    SELECT 
        p.product_id,
        p.product_name,
        p.category,
        DATE(STR_TO_DATE(pur.purchase_date, '%d-%m-%Y %H:%i')) AS purchase_day,
        SUM(pur.quantity) AS total_purchases
    FROM 
        Purchase pur
    JOIN 
        Products p ON pur.product_id = p.product_id
    GROUP BY 
        p.product_id, p.product_name, p.category,purchase_day
),
MovingAvg AS (
    SELECT 
        dp.product_id,
        dp.product_name,
        dp.category,
        dp.purchase_day,
        dp.total_purchases,
        AVG(dp.total_purchases) OVER (
            PARTITION BY dp.product_name,dp.category
            ORDER BY dp.purchase_day
            ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
        ) AS moving_avg
    FROM 
        DailyPurchases dp
)
SELECT 
    product_id,
    product_name,
    category,
    purchase_day,
    moving_avg
FROM 
    MovingAvg
ORDER BY 
    product_id, purchase_day asc;
-- Insights:  A sharp increase in the moving average may indicate successful marketing campaigns, discounts, or seasonal trends.
-- Products with high volatility in purchases might require adjustments in pricing, marketing, or stock levels.







-- Which customer had high 7 day MA
WITH DailyCustomerPurchases AS (
    SELECT 
        pur.customer_id,
        DATE(pur.purchase_date) AS purchase_day,
        SUM(pur.total_amount) AS total_purchases
    FROM 
        Purchase pur
    GROUP BY 
        pur.customer_id, purchase_day
),
MovingAvg AS (
    SELECT 
        dcp.customer_id,
        dcp.purchase_day,
        dcp.total_purchases,
        AVG(dcp.total_purchases) OVER (
            PARTITION BY dcp.customer_id
            ORDER BY dcp.purchase_day
            ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
        ) AS moving_avg
    FROM 
        DailyCustomerPurchases dcp
)
SELECT 
    ma.customer_id,
    c.first_name,
    c.last_name,
    MAX(ma.moving_avg) AS highest_7day_MA
FROM 
    MovingAvg ma
JOIN 
    Customer c ON ma.customer_id = c.customer_id
GROUP BY 
    ma.customer_id, c.first_name, c.last_name
ORDER BY 
    highest_7day_MA DESC
LIMIT 1;
-- Insights: Ben has the highest moving average and more likely frequent buyer or bulk purchaser.
--  contribute significantly to total revenue and can be targeted for loyalty programs.







-- WITH CategoryRevenue AS (
--     SELECT 
--         p.PRODUCT_NAME,
--         SUM(pur.total_amount) AS total_revenue
--     FROM 
--         Purchase pur
--     JOIN 
--         Products p ON pur.product_id = p.product_id
--     GROUP BY 
--         p.product_name
--     ORDER BY 
--         total_revenue DESC
--     LIMIT 5
-- ),
-- CustomersWithPurchases AS (
--     SELECT DISTINCT 
--         pur.customer_id
--     FROM 
--         Purchase pur
--     JOIN 
--         Products p ON pur.product_id = p.product_id
--     WHERE 
--         p.product_name IN (SELECT category FROM CategoryRevenue)
-- )
-- SELECT 
--     c.customer_id,
--     c.first_name,
--     c.last_name,
--     c.email,
--     c.phone_number
-- FROM 
--     Customer c
-- LEFT JOIN 
--     CustomersWithPurchases cp ON c.customer_id = cp.customer_id
-- WHERE 
--     cp.customer_id IS NULL
-- ORDER BY 
--     c.customer_id;








-- M. Nested Queries:
-- The top 5 most popular categories
SELECT 
    p.product_name,
    p.category,
    SUM(pur.quantity) AS total_quantity_sold
FROM 
    Purchase pur
JOIN 
    Products p ON pur.product_id = p.product_id
GROUP BY 
    p.product_name, p.category
ORDER BY 
    total_quantity_sold DESC
LIMIT 5;
-- Insights: It shows the demand for the product and need to maintain inventory






-- WITH ProductsToExclude AS (
--     SELECT 
--         p.product_id
--     FROM 
--         Products p
--     WHERE 
--         (p.product_name = 'Milk' AND p.category IN ('Meat', 'Dairy'))
--         OR (p.product_name = 'Cheese' AND p.category = 'Dairy')
--         OR (p.product_name = 'Bread' AND p.category IN ('Dairy', 'Meat'))
-- )
-- SELECT 
--     Distinct c.customer_id,
--     c.first_name,
--     c.last_name,
--     c.email,
--     c.phone_number
-- FROM 
--     Customer c
-- LEFT JOIN 
--     Purchase pur ON c.customer_id = pur.customer_id
-- LEFT JOIN 
--     ProductsToExclude pte ON pur.product_id = pte.product_id
-- WHERE 
--     pte.product_id IS NULL  -- Ensures that the customer has not bought any of the specified products
-- ORDER BY 
--     c.customer_id;







-- customers who have never purchased products from the top 5 most popular categories.
WITH ProductsToExclude AS (
    SELECT 
        p.product_id
    FROM 
        Products p
    WHERE 
        (p.product_name = 'Milk' AND p.category IN ('Meat', 'Dairy'))
        OR (p.product_name = 'Cheese' AND p.category = 'Dairy')
        OR (p.product_name = 'Bread' AND p.category IN ('Dairy', 'Meat'))
),
CustomersWithExcludedPurchases AS (
    SELECT DISTINCT
        pur.customer_id
    FROM 
        Purchase pur
    JOIN 
        ProductsToExclude pte ON pur.product_id = pte.product_id
)
SELECT 
    c.customer_id,
    c.first_name,
    c.last_name,
    c.email,
    c.phone_number
FROM 
    Customer c
WHERE 
    c.customer_id NOT IN (SELECT customer_id FROM CustomersWithExcludedPurchases)
ORDER BY 
    c.customer_id;
-- Insights: These clients needs to be given discount for those pouplar categories to make them buy







-- Identify products purchased only once in the entire dataset.
SELECT 
    p.product_id,
    p.product_name,
    p.category,
    COUNT(pur.purchase_id) AS purchase_count
FROM 
    Purchase pur
JOIN 
    Products p ON pur.product_id = p.product_id
GROUP BY 
    p.product_id, p.product_name, p.category
HAVING 
    COUNT(pur.purchase_id) = 1
ORDER BY 
    p.product_id;
-- Insights:  We need give more discount or offers to these products







-- SELECT 
--     YEAR(pur.purchase_date) AS year,
--     MONTH(pur.purchase_date) AS month,
--     SUM(pur.total_amount) AS total_sales_volume
-- FROM 
--     Purchase pur
-- GROUP BY 
--     YEAR(pur.purchase_date), MONTH(pur.purchase_date)
-- ORDER BY 
--     total_sales_volume DESC
-- LIMIT 1;







-- N. Date Analysis:
-- Identify the month with the highest total sales volume.
SELECT 
    MONTH(STR_TO_DATE(pur.purchase_date, '%d-%m-%Y %H:%i')) AS month,
    SUM(pur.quantity) AS total_sales_volume
FROM 
    Purchase pur
GROUP BY 
    MONTH(STR_TO_DATE(pur.purchase_date, '%d-%m-%Y %H:%i'))
ORDER BY 
    total_sales_volume DESC
LIMIT 1;
-- Insights:  April month the sales has peaked. All the products inventory should be stocked






-- Calculate the year-over-year growth of total sales.
WITH YearlySales AS (
    SELECT 
        YEAR(STR_TO_DATE(p.purchase_date, '%d-%m-%Y %H:%i')) AS year,
        SUM(p.total_amount) AS total_sales
    FROM 
        Purchase p
    GROUP BY 
        YEAR
)
SELECT 
    current.year,
    ROUND(current.total_sales,0) AS current_year_sales,
    ROUND(previous.total_sales,0) AS previous_year_sales,
    ROUND(((current.total_sales - previous.total_sales) / previous.total_sales) * 100,0) AS yoy_growth_percentage
FROM 
    YearlySales current
JOIN 
    YearlySales previous ON current.year = previous.year + 1
ORDER BY 
    current.year DESC;
-- Insights: 2023, the sales growth percentage is peaked which is a good sign and need to increase in coming years




  
-- O. Join with Aggregation:
-- Join all three datasets to find the total revenue per product per customer.
SELECT 
    c.customer_id,
    CONCAT(c.first_name, ' ', c.last_name) AS customer_name,
    p.product_id,
    pr.product_name,
    pr.category,
    pr.price_per_unit,
    p.quantity,
    p.purchase_date,
    SUM(p.total_amount) AS total_revenue
FROM Purchase p
JOIN Customer c ON p.customer_id = c.customer_id
JOIN Products pr ON p.product_id = pr.product_id
GROUP BY c.customer_id, customer_name, p.product_id, pr.product_name, pr.category,  pr.price_per_unit, p.quantity, p.purchase_date
ORDER BY total_revenue DESC;
-- Insights: This helps to find which client contributed to the most sales and for which product.alter





-- P. Customer Retention:
-- Find the percentage of repeat customers in the dataset.
WITH Repeat_Customers AS (
    SELECT customer_id
    FROM Purchase
    GROUP BY customer_id
    HAVING COUNT(purchase_id) > 1
)
SELECT 
    (COUNT(distinct rc.customer_id)  / COUNT( p.customer_id)) * 100.0 AS repeat_customer_percentage
FROM Purchase p
LEFT JOIN Repeat_Customers rc ON p.customer_id = rc.customer_id;
-- Insights:  This helps to understand that very less people are visiting again. Need to introduce plans to frequently make clients visit alter








-- SELECT COUNT(customer_id) AS total_customers
-- FROM Purchase;

-- SELECT c.customer_id, row_number() over (order by c.customer_id desc) as row_num
-- FROM Customer c
-- LEFT JOIN Purchase p ON c.customer_id = p.customer_id
-- WHERE p.purchase_id IS NULL;


-- SELECT customer_id, COUNT(*), row_number() OVER (ORDER BY COUNT(*) DESC) AS row_numbers 
-- FROM Purchase 
-- GROUP BY customer_id 
-- HAVING COUNT(*) >1;

-- SELECT COUNT(DISTINCT customer_id) AS total_unique_customers
-- FROM Purchase;


-- SELECT COUNT(*) AS total_repeat_customers
-- FROM (
--     SELECT customer_id
--     FROM Purchase
--     GROUP BY customer_id
--     HAVING COUNT(customer_id) > 1
-- ) AS repeat_customers;

-- SELECT 
--     customer_id, 
--     COUNT(*) AS purchase_count,
--     ROW_NUMBER() OVER (ORDER BY COUNT(*) DESC) AS row_numbers
-- FROM Purchase
-- GROUP BY customer_id
-- HAVING COUNT(*) > 1;









-- Calculate the average number of days between purchases for repeat customers.
WITH Repeat_Customers AS (
    SELECT customer_id
    FROM Purchase
    GROUP BY customer_id
    HAVING COUNT(purchase_id) > 1
),
Purchase_With_Prev AS (
    SELECT 
        p.customer_id,
	    (p.purchase_date) as purchase_date,
        LAG(purchase_date) OVER (PARTITION BY p.customer_id ORDER BY purchase_date) AS prev_purchase_date
    FROM Purchase p
    INNER JOIN Repeat_Customers rc ON p.customer_id = rc.customer_id
)
SELECT 
    Customer_id,
    AVG(DATEDIFF(purchase_date, prev_purchase_date)) AS avg_days_between_purchases
FROM Purchase_With_Prev
WHERE prev_purchase_date IS NOT NULL
group by Customer_id
order by avg_days_between_purchases asc;






-- Q. Time Series Analysis:
-- Find the products with the highest growth in purchase frequency over time (average purchase YOY).
WITH Yearly_Product_Purchases AS (
    SELECT 
        p.product_name,
        p.category,
        EXTRACT(YEAR FROM pu.purchase_date) AS purchase_year,
        SUM(pu.quantity) AS total_quantity
    FROM Purchase pu
    JOIN Products p ON pu.product_id = p.product_id
    GROUP BY p.product_name, p.category, purchase_year
),
Product_Growth AS (
    SELECT 
        y1.product_name,
        y1.category,
        y1.purchase_year,
        y1.total_quantity AS current_year_quantity,
        y2.total_quantity AS previous_year_quantity,
        CASE 
            WHEN y2.total_quantity > 0 
            THEN ((y1.total_quantity - y2.total_quantity)  / y2.total_quantity)*100
            ELSE NULL
        END AS yoy_growth
    FROM Yearly_Product_Purchases y1
    LEFT JOIN Yearly_Product_Purchases y2
        ON y1.product_name = y2.product_name
        AND y1.category = y2.category
        AND y1.purchase_year = y2.purchase_year + 1
)
SELECT 
    pg.product_name,
    pg.category,
    ROUND(AVG(pg.yoy_growth),0) AS avg_yoy_growth
FROM Product_Growth pg
WHERE pg.yoy_growth IS NOT NULL
GROUP BY pg.product_name, pg.category
ORDER BY avg_yoy_growth DESC;
-- Insights:  Cheese and diary has the highest avg yoy growth






-- select p.product_id,	p.purchase_date,	p.quantity, pr.product_name, 	pr.category, row_number() over (order by product_id )
-- from products pr join purchase p on pr.product_id = p.product_id;








-- R. Subqueries:
-- Identify customers whose total expenditure is above the average expenditure of all customers.
WITH Customer_Expenditure AS (
    SELECT 
        p.customer_id, 
        CONCAT(C.FIRST_NAME,' ',C.LAST_NAME) as customer_name,
        SUM(p.total_amount) AS total_spent
    FROM Purchase p join customer c on p.customer_id=c.customer_id
    GROUP BY p.customer_id, customer_name
),
Average_Expenditure AS (
    SELECT AVG(total_spent) AS avg_expenditure
    FROM Customer_Expenditure
)
SELECT ce.customer_id, ce.customer_name, ce.total_spent, ae.avg_expenditure
FROM Customer_Expenditure ce
CROSS JOIN Average_Expenditure ae
WHERE ce.total_spent > ae.avg_expenditure
ORDER BY ce.total_spent DESC;
-- Insights:  These are the valuable client list who purchasse more than the avg of all clients






-- Find products with a purchase amount higher than the average purchase amount across all products.
WITH Product_Purchase AS (
    SELECT 
        p.product_id, 
        pr.product_name,
        pr.category,
        pr.product_description,
        SUM(p.total_amount) AS total_revenue
    FROM Purchase p
    JOIN Products pr ON p.product_id = pr.product_id
    GROUP BY p.product_id, pr.product_name, pr.category, pr.product_description
),
Average_Purchase AS (
    SELECT AVG(total_revenue) AS avg_revenue
    FROM Product_Purchase
)
SELECT pp.product_id, pp.product_name, pp.category, pp.product_description, pp.total_revenue, ap.avg_revenue
FROM Product_Purchase pp
CROSS JOIN Average_Purchase ap
WHERE pp.total_revenue > ap.avg_revenue
ORDER BY pp.total_revenue DESC;
-- Insights: These product categories sales is more than the avg of all the products sales revenue






-- S. Correlated Subqueries:
-- Find the purchase dates where a customer made a purchase larger than their average purchase amount.
WITH Customer_Avg_Purchase AS (
    SELECT 
        customer_id, 
        AVG(total_amount) AS avg_purchase_amount
    FROM Purchase
    GROUP BY customer_id
)
SELECT p.customer_id, p.purchase_date, p.total_amount, cap.avg_purchase_amount
FROM Purchase p
JOIN Customer_Avg_Purchase cap 
    ON p.customer_id = cap.customer_id
WHERE p.total_amount > cap.avg_purchase_amount
ORDER BY p.customer_id, p.purchase_date;
-- Insights: In these dates the customer has bought more than he normally purchases







-- WITH Product_Frequency AS (
--     SELECT 
--         p.product_id, 
--         pr.product_name, 
--         pr.category,
--         COUNT(p.purchase_id) AS purchase_count
--     FROM Purchase p
--     JOIN Products pr ON p.product_id = pr.product_id
--     GROUP BY p.product_id, pr.product_name, pr.category
-- ),
-- Category_Avg_Frequency AS (
--     SELECT 
--         category, 
--         AVG(purchase_count) AS avg_category_frequency
--     FROM Product_Frequency
--     GROUP BY category
-- )
-- SELECT pf.product_id, pf.product_name, pf.category, pf.purchase_count, caf.avg_category_frequency
-- FROM Product_Frequency pf
-- JOIN Category_Avg_Frequency caf ON pf.category = caf.category
-- WHERE pf.purchase_count > caf.avg_category_frequency
-- ORDER BY pf.category, pf.purchase_count DESC;

-- WITH Product_Frequency AS (
--     SELECT 
--         p.product_id, 
--         pr.product_name, 
--         pr.category,
--         COUNT(DISTINCT DATE(p.purchase_date)) AS purchase_frequency
--     FROM Purchase p
--     JOIN Products pr ON p.product_id = pr.product_id
--     GROUP BY p.product_id, pr.product_name, pr.category
-- ),
-- Category_Avg_Frequency AS (
--     SELECT 
--         category, 
--         AVG(purchase_frequency) AS avg_category_frequency
--     FROM Product_Frequency
--     GROUP BY category
-- )
-- SELECT pf.product_id, pf.product_name, pf.category, pf.purchase_frequency, caf.avg_category_frequency
-- FROM Product_Frequency pf
-- JOIN Category_Avg_Frequency caf ON pf.category = caf.category
-- WHERE pf.purchase_frequency > caf.avg_category_frequency
-- ORDER BY pf.category, pf.purchase_frequency DESC;



-- SELECT 
--        
--         pr.product_name, 
--         pr.category,
--         COUNT(DATE(p.purchase_date)) AS purchase_frequency
--     FROM Purchase p
--     JOIN Products pr ON p.product_id = pr.product_id
--     GROUP BY  pr.product_name, pr.category;








-- List all products that were purchased more frequently than the average frequency of products in their category.    
WITH Product_Frequency AS (
    SELECT 
        pr.product_name, 
        pr.category,
        COUNT(DATE(p.purchase_date)) AS purchase_frequency
    FROM Purchase p
    JOIN Products pr ON p.product_id = pr.product_id
    GROUP BY  pr.product_name, pr.category
),
Category_Avg_Frequency AS (
    SELECT 
        category, 
        AVG(purchase_frequency) AS avg_category_frequency
    FROM Product_Frequency
    GROUP BY category
)
SELECT  pf.product_name, pf.category, pf.purchase_frequency, caf.avg_category_frequency
FROM Product_Frequency pf
JOIN Category_Avg_Frequency caf ON pf.category = caf.category
WHERE pf.purchase_frequency > caf.avg_category_frequency
ORDER BY pf.category, pf.purchase_frequency DESC;
-- Insight: These product category is moving fast so inventory needs to maintained regularly







-- T. Date Functions:
-- Extract the day, month, and year from the purchase date, and group total purchases by month across all years
SELECT     MONTH(pur.purchase_date) AS month,    ROUND(SUM(pur.total_amount),0) AS total_sales_volume 
FROM     Purchase pur
GROUP BY     MONTH(pur.purchase_date)
ORDER BY    total_sales_volume  	DESC;
-- Insights:  This shows the perfoming month. It helps to manage the inventory and decide to give offers on month that needs to increase sales 




