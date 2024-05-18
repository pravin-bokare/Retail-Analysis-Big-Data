from pyspark.sql.functions import *

def filter_closed_orders(orders_df):
    return orders_df.filter("order_status = 'CLOSED'")

def join_orders_customers(order_df, customer_df):
    return order_df.join(customer_df, 'customer_id')

def count_orders_state(joined_df):
    return joined_df.groupBy('state').count()

def filters_orders_generic(orders_df, status):
    return orders_df.filter("order_status = '{}'".format(status))