#!/usr/bin/env python
# coding: utf-8

# In[307]:


# import library
import pandas as pd
import mysql.connector
import os
import pandas as pd
import numpy as np
import seaborn as sns 
import matplotlib.pyplot as plt
import plotly.express as px
import sqlite3

# download important library
get_ipython().system('pip install pymysql')
get_ipython().system('pip install pymysql pandas')


# In[2]:


# List of CSV files and their corresponding table names
csv_files = [
    ('customers.csv', 'customers'),
    ('orders.csv', 'orders'),
    ('sellers.csv', 'sellers'),
    ('products.csv', 'products'),
    ('order_items.csv', 'order_items'),
    ('payments.csv', 'payments'),
    ('geolocation.csv', 'geolocation')
]


# # make connection between MYSQL and python

# In[380]:


import pymysql

conn = pymysql.connect(
    host='localhost',
    user='root',
    password='Nishant@2001',
    database='e_Commerce'
)

cur = conn.cursor()


# # upload files in database 

# In[20]:


folder_path = '/Users/nishantmehra/Desktop/data'

def get_sql_type(dtype):
    if pd.api.types.is_integer_dtype(dtype):
        return 'INT'
    elif pd.api.types.is_float_dtype(dtype):
        return 'FLOAT'
    elif pd.api.types.is_bool_dtype(dtype):
        return 'BOOLEAN'
    elif pd.api.types.is_datetime64_any_dtype(dtype):
        return 'DATETIME'
    else:
        return 'TEXT'

for csv_file, table_name in csv_files:
    file_path = os.path.join(folder_path, csv_file)
    
    # Read the CSV file into a pandas DataFrame
    df = pd.read_csv(file_path)
    
    # Replace NaN with None to handle SQL NULL
    df = df.where(pd.notnull(df), None)
    
    # Debugging: Check for NaN values
    print(f"Processing {csv_file}")
    print(f"NaN values before replacement:\n{df.isnull().sum()}\n")

    # Clean column names
    df.columns = [col.replace(' ', '_').replace('-', '_').replace('.', '_') for col in df.columns]

    # Generate the CREATE TABLE statement with appropriate data types
    columns = ', '.join([f'`{col}` {get_sql_type(df[col].dtype)}' for col in df.columns])
    create_table_query = f'CREATE TABLE IF NOT EXISTS `{table_name}` ({columns})'
    cursor.execute(create_table_query)

    # Insert DataFrame data into the MySQL table
    for _, row in df.iterrows():
        # Convert row to tuple and handle NaN/None explicitly
        values = tuple(None if pd.isna(x) else x for x in row)
        sql = f"INSERT INTO `{table_name}` ({', '.join(['`' + col + '`' for col in df.columns])}) VALUES ({', '.join(['%s'] * len(row))})"
        cursor.execute(sql, values)

    # Commit the transaction for the current CSV file
    conn.commit()

# Close the connection
conn.close()


# # show All Tables in e_Commerce database

# In[425]:


query = ("""
SHOW tables
""")
cur.execute(query)
data = cur.fetchall()
data = pd.DataFrame(data, columns = ["Tables"])
data


# #                                  Data Modelling / Relationship Between Tables
# ![Screenshot%202024-07-13%20at%2013.10.42.png](attachment:Screenshot%202024-07-13%20at%2013.10.42.png)
# 

# # Calculate the moving average of order values for each customer over their order history.

# In[426]:


query = ("""
select customer_id, 
order_purchase_timestamp, 
AVG(payment) OVER(partition by customer_id order by order_purchase_timestamp ROWS between 2 preceding and current row) as moving_avg
from (
    select orders.customer_id, orders.order_purchase_timestamp, payments.payment_value as payment
    from orders JOIN payments 
    ON orders.order_id = payments.order_id
) as t
""")
cur.execute(query)
data = cur.fetchall()
data = pd.DataFrame(data, columns = ["customer_id","order_purchase_timestamp","moving_avg"])
data


# # Calculate the cumulative sales per month for each year.

# In[427]:


query = ("""
select CONCAT(CAST(year AS CHAR), '-', LPAD(CAST(month AS CHAR), 2, '0')) AS month_years,
SUM(price) over(order by year, month) as cum_payment
from (
SELECT 
    year(orders.order_purchase_timestamp) AS year, 
    month(orders.order_purchase_timestamp) AS month,
    SUM(payments.payment_value) AS price
FROM orders JOIN payments
ON orders.order_id = payments.order_id
group by year, month    
order by year, month asc
) as t
""")
cur.execute(query)
data = cur.fetchall()
data = pd.DataFrame(data, columns = ["month_years","cum_payment"])
data


# In[431]:


sns.lineplot(data, x = "month_years", y = "cum_payment")
plt.title("Moving Price Month Wise")
plt.xticks(["2016-09","2017-04","2017-10","2018-04","2018-10"]) 
plt.show()


# # Calculate the year-over-year growth rate of total sales.

# In[432]:


query = ("""
with year_price AS (
        select 
            YEAR(orders.order_purchase_timestamp) as year,
            SUM(order_items.price + order_items.freight_value) as price
        FROM orders 
        JOIN order_items 
ON orders.order_id = order_items.order_id
GROUP BY year(orders.order_purchase_timestamp)
)
select year, 
sum(price) OVER(partition by year ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) as cum_price
from year_price
ORDER BY year

""")
cur.execute(query)
data = cur.fetchall()
data = pd.DataFrame(data, columns = ["year","cum_price"])
data


# In[433]:


sns.lineplot(data, x = "year", y = "cum_price", marker='o', linewidth=2.5, markersize=8)
plt.title("Relation Between Year and Cum_price")
plt.xticks([2016, 2017, 2018]) 
plt.grid(True, alpha=0.5)
plt.show()


# # Calculate the retention rate of customers, defined as the percentage of customers who make another purchase within 6 months of their first purchase.

# In[424]:


query = ("""
with a as (select customers.customer_id,
min(orders.order_purchase_timestamp) first_order
from customers join orders
on customers.customer_id = orders.customer_id
group by customers.customer_id),

b as (select a.customer_id, count(distinct orders.order_purchase_timestamp) next_order
from a join orders
on orders.customer_id = a.customer_id
and orders.order_purchase_timestamp > first_order
and orders.order_purchase_timestamp < 
date_add(first_order, interval 6 month)
group by a.customer_id) 

select 100 * (count( distinct a.customer_id)/ count(distinct b.customer_id)) 
from a left join b 
on a.customer_id = b.customer_id
""")
cur.execute(query)
data = cur.fetchall()
data = pd.DataFrame(data)
data


# # Identify the top 3 customers who spent the most money in each year.

# In[436]:


query = ("""select years, customer_id, payment, d_rank
from
(select year(orders.order_purchase_timestamp) years,
orders.customer_id,
sum(payments.payment_value) payment,
dense_rank() over(partition by year(orders.order_purchase_timestamp)
order by sum(payments.payment_value) desc) d_rank
from orders join payments 
on payments.order_id = orders.order_id
group by year(orders.order_purchase_timestamp),
orders.customer_id) as a
where d_rank <= 3 """)
cur.execute(query)
data = cur.fetchall()
data = pd.DataFrame(data, columns = ["year","customer_id","payment","Rank"])
data


# In[437]:


sns.barplot(x = "customer_id", y = "payment", data = data, hue = "year")
plt.xticks(rotation = 90)
plt.show()


# # Identify the correlation between product price and the number of times a product has been purchased.

# In[413]:


query = ("""
WITH product_purchases AS (
    SELECT 
    product_id,
    AVG(price) AS avg_price,
    COUNT(order_id) AS num_purchases
    FROM order_items
    GROUP BY product_id
)
SELECT 
ROUND(avg_price,2) as avg_price,
num_purchases
FROM product_purchases
order by num_purchases desc
""")
cur.execute(query)
data = cur.fetchall()
data = pd.DataFrame(data, columns = ["avg_price","num_purchases"])
data


# In[414]:


fig, ax = plt.subplots(1, 3, figsize=(18, 6))

# first box plot for num_purchases
sns.boxplot(data=t7, y="num_purchases", ax=ax[0])
ax[0].set_title("Boxplot of Number of Purchases")
ax[0].set_xlabel("Number of Purchases")
ax[0].set_ylabel("Values")

# second box plot for avg_price
sns.boxplot(data=t7, y="avg_price", ax=ax[1])
ax[1].set_title("Boxplot of Average Price")
ax[1].set_xlabel("Average Price")
ax[1].set_ylabel("Values")

# scatter plot for avg_price vs num_purchases
sns.scatterplot(data=t7, x="avg_price", y="num_purchases", ax=ax[2])
ax[2].set_title("Relation Between Avg Price and Number of Purchases")

plt.show()


# # Calculate the total revenue generated by each seller, and rank them by revenue

# In[386]:


query = ("""
select *, dense_rank() over(order by revenue desc) as ranks 
from
(select order_items.seller_id, sum(payments.payment_value)
revenue from order_items join payments
on order_items.order_id = payments.order_id
group by order_items.seller_id) as t
""")

cur.execute(query)
data = cur.fetchall()
data = pd.DataFrame(data,columns = ["seller_id", "revenue","Rank"])
data


# In[390]:


sns.barplot(data = data.head(10) , x = "revenue", y = "seller_id")
plt.title("Most Revenue of Seller")
plt.show()


# # Calculate the percentage of total revenue contributed by each product category.

# In[392]:


query = ("""
WITH total_sum AS (
    SELECT SUM(price) AS p FROM order_items
)

SELECT 
    products.product_category, 
    ROUND((SUM(order_items.price) / (SELECT p FROM total_sum)) * 100,2) AS "percentage_of_total(%)"
FROM products
JOIN order_items ON products.product_id = order_items.product_id
GROUP BY products.product_category
ORDER BY SUM(order_items.price) DESC;
""")
cur.execute(query)
data = cur.fetchall()
data = pd.DataFrame(data, columns = ["category","price (%)"])
data


# # Find the average number of products per order, grouped by customer city.

# In[399]:


query = ("""
with count_per_order AS ( 
    SELECT 
        COUNT(order_items.order_id) as oc, 
        orders.customer_id
    FROM orders JOIN order_items
    ON orders.order_id = order_items.order_id
group by orders.customer_id, orders.order_id 
)

select customers.customer_city, ROUND(AVG(count_per_order.oc),2) as avg_count
FROM count_per_order 
JOIN customers 
ON count_per_order.customer_id = customers.customer_id
GROUP BY customers.customer_city
order by ROUND(AVG(count_per_order.oc),2) desc
""")
cur.execute(query)
data = cur.fetchall()
data = pd.DataFrame(data, columns = ["customer_city","avg_count"])
data


# In[400]:


sns.barplot(data = data.head(20), x = "avg_count", y = "customer_city")
plt.title("City Wise Avg Order Count")
plt.show()


# # Calculate the number of orders per month in 2018.

# In[396]:


query = ("""
SELECT t.count, t.month FROM (
SELECT COUNT(order_id) as count, MONTHNAME(order_purchase_timestamp) as month, YEAR(order_purchase_timestamp) as year
FROM orders
GROUP BY MONTHNAME(order_purchase_timestamp),YEAR(order_purchase_timestamp)
) as t
WHERE t.year = 2018
order by t.count desc
""")
cur.execute(query)
data = cur.fetchall()
data = pd.DataFrame(data, columns = ["count","month"])
data


# In[397]:


sns.barplot(data = data, x = "month", y = "count")
plt.title("2018 Order Count Month Wise")
plt.xticks(rotation = 45)
plt.show()


# # Calculate the percentage of orders that were paid in installments.

# In[403]:


query = (""" 
select (sum(case when payment_installments > 1 then 1 else 0 end) / COUNT(*))*100 as Average_Count
from payments;
""")
cur.execute(query)
data = cur.fetchall()
data = pd.DataFrame(data, columns = ["Avg Count"])
data


# # Count the number of customers from each state.

# In[404]:


query = ("""
SELECT customer_state, COUNT(customer_unique_id) as customer_count
FROM customers
GROUP BY customer_state
order by COUNT(customer_unique_id) desc
""")
cur.execute(query)
data = cur.fetchall()
data = pd.DataFrame(data, columns = ["customer_state","customer_count"])
data


# In[405]:


sns.barplot(data = data, x = "customer_count" ,y = "customer_state")
plt.title("State Wise Customer Count")
plt.show()


# # Find the total sales per category.

# In[438]:


query = ("""
select products.product_category as category, 
SUM(payments.payment_value) as sales
from products 
JOIN order_items
ON products.product_id = order_items.product_id
JOIN payments
ON payments.order_id = order_items.order_id
GROUP BY category
order by SUM(payments.payment_value) desc
""")
cur.execute(query)
data = cur.fetchall()
data = pd.DataFrame(data, columns = ["category","sales"])
data


# In[439]:


sns.barplot(data = data.head(10), x = "sales", y = "category")
plt.title("Category wise sales")
plt.show()


# # Count the number of orders placed in 2017.

# In[411]:


query = ("""
SELECT COUNT(*) as order_count, YEAR(order_approved_at) as year
FROM orders
WHERE YEAR(order_approved_at) = 2017
GROUP BY YEAR(order_approved_at)
""")
cur.execute(query)
data = cur.fetchall()
data = pd.DataFrame(data, columns = ["order_count","year"])
data


# # List all unique cities where customers are located.

# In[412]:


query = ("""
SELECT DISTINCT(customer_city) 
from customers
""")
cur.execute(query)
data = cur.fetchall()
data = pd.DataFrame(data, columns = ["customer_city"])
data

