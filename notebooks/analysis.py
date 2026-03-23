import sqlite3
import pandas as pd

conn = sqlite3.connect('data/ecommerce.db')

# -------- TASK 1: Total Revenue --------
print("\n--- Total Revenue ---")
query1 = """
SELECT SUM(total_amount) AS total_revenue
FROM orders;
"""
df1 = pd.read_sql(query1, conn)
print(df1)

# -------- TASK 2: Revenue by Day --------
print("\n--- Revenue by Day ---")
query2 = """
SELECT order_day, SUM(total_amount) AS revenue
FROM orders
GROUP BY order_day
ORDER BY revenue DESC;
"""
df2 = pd.read_sql(query2, conn)
print(df2)

# -------- TASK 3: Top Customers --------
print("\n--- Top Customers ---")
query3 = """
SELECT customer_id, SUM(total_amount) AS total_spent
FROM orders
GROUP BY customer_id
ORDER BY total_spent DESC
LIMIT 10;
"""
df3 = pd.read_sql(query3, conn)
print(df3)

# -------- TASK 4: Top Products --------
print("\n--- Top Products ---")
query4 = """
SELECT p.product_name, SUM(oi.quantity * oi.price) AS revenue
FROM order_items oi
JOIN products p ON oi.product_id = p.product_id
GROUP BY p.product_name
ORDER BY revenue DESC
LIMIT 10;
"""


df4 = pd.read_sql("SELECT * FROM order_items LIMIT 5", conn)
print(df4.columns)

print("\n--- Customer Segmentation ---")

query5 = """
SELECT 
    customer_id,
    SUM(total_amount) AS total_spent,
    CASE 
        WHEN SUM(total_amount) > 300000 THEN 'High Value'
        WHEN SUM(total_amount) BETWEEN 100000 AND 300000 THEN 'Medium Value'
        ELSE 'Low Value'
    END AS customer_segment
FROM orders
GROUP BY customer_id
ORDER BY total_spent DESC;
"""

df5 = pd.read_sql(query5, conn)
print(df5.head(20))

print("\n--- Segment Distribution ---")

query6 = """
SELECT 
    customer_segment,
    COUNT(*) AS num_customers
FROM (
    SELECT 
        customer_id,
        CASE 
            WHEN SUM(total_amount) > 300000 THEN 'High Value'
            WHEN SUM(total_amount) BETWEEN 100000 AND 300000 THEN 'Medium Value'
            ELSE 'Low Value'
        END AS customer_segment
    FROM orders
    GROUP BY customer_id
)
GROUP BY customer_segment;
"""

df6 = pd.read_sql(query6, conn)
print(df6)


print("\n--- Revenue Contribution by Segment ---")

query7 = """
SELECT 
    customer_segment,
    SUM(total_spent) AS segment_revenue
FROM (
    SELECT 
        customer_id,
        SUM(total_amount) AS total_spent,
        CASE 
            WHEN SUM(total_amount) > 300000 THEN 'High Value'
            WHEN SUM(total_amount) BETWEEN 100000 AND 300000 THEN 'Medium Value'
            ELSE 'Low Value'
        END AS customer_segment
    FROM orders
    GROUP BY customer_id
)
GROUP BY customer_segment
ORDER BY segment_revenue DESC;
"""

df7 = pd.read_sql(query7, conn)
print(df7)


print("\n--- Customer Spending Distribution ---")

query8 = """
SELECT 
    MIN(total_spent) AS min_spent,
    MAX(total_spent) AS max_spent,
    AVG(total_spent) AS avg_spent
FROM (
    SELECT customer_id, SUM(total_amount) AS total_spent
    FROM orders
    GROUP BY customer_id
);
"""

df8 = pd.read_sql(query8, conn)
print(df8)

print("\n--- Improved Customer Segmentation ---")

query9 = """
WITH customer_spending AS (
    SELECT 
        customer_id,
        SUM(total_amount) AS total_spent
    FROM orders
    GROUP BY customer_id
),
ranked_customers AS (
    SELECT *,
        NTILE(10) OVER (ORDER BY total_spent DESC) AS decile
    FROM customer_spending
)
SELECT 
    customer_id,
    total_spent,
    CASE 
        WHEN decile = 1 THEN 'High Value'
        WHEN decile <= 4 THEN 'Medium Value'
        ELSE 'Low Value'
    END AS customer_segment
FROM ranked_customers
ORDER BY total_spent DESC;
"""

df9 = pd.read_sql(query9, conn)
print(df9.head(20))

print("\n--- Final Segment Revenue ---")

query_final = """
WITH customer_spending AS (
    SELECT 
        customer_id,
        SUM(total_amount) AS total_spent
    FROM orders
    GROUP BY customer_id
),
ranked_customers AS (
    SELECT *,
        NTILE(10) OVER (ORDER BY total_spent DESC) AS decile
    FROM customer_spending
)
SELECT 
    CASE 
        WHEN decile = 1 THEN 'High Value'
        WHEN decile <= 4 THEN 'Medium Value'
        ELSE 'Low Value'
    END AS customer_segment,
    COUNT(*) AS num_customers,
    SUM(total_spent) AS total_revenue
FROM ranked_customers
GROUP BY customer_segment
ORDER BY total_revenue DESC;
"""

df_final = pd.read_sql(query_final, conn)
print(df_final)

df_final.to_csv('data/customer_segments.csv', index=False)
df1.to_csv('data/revenue_summary.csv', index=False)
df3.to_csv('data/top_customers.csv', index=False)
df4.to_csv('data/top_products.csv', index=False)
df_final.to_csv('data/customer_segments.csv', index=False)

query_time = """
SELECT 
    order_date,
    SUM(total_amount) AS revenue
FROM orders
GROUP BY order_date
ORDER BY order_date;
"""

df_time = pd.read_sql(query_time, conn)
df_time.to_csv('data/revenue_trend.csv', index=False)

query_time = """
SELECT 
    order_date,
    SUM(total_amount) AS revenue
FROM orders
GROUP BY order_date
ORDER BY order_date;
"""

df_time = pd.read_sql(query_time, conn)
df_time.to_csv('data/revenue_trend.csv', index=False)

query_products = """
SELECT 
    p.product_name,
    SUM(oi.quantity * oi.unit_price_after_discount) AS revenue
FROM order_items oi
JOIN products p ON oi.product_id = p.product_id
GROUP BY p.product_name
ORDER BY revenue DESC;
"""

df_products = pd.read_sql(query_products, conn)
df_products.to_csv('data/top_products.csv', index=False)