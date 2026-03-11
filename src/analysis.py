#src/analysis.py
from pyspark.sql.functions import col, sum as spark_sum, desc

def category_sales(df):
    """
    Calculates total sales per product category.
    """
    result = df.groupBy("Category") \
        .agg(spark_sum("TotalSales").alias("CategorySales")) \
        .orderBy(desc("CategorySales"))
    print("📊 Total sales per category calculated.")
    return result


def top_selling_categories(df):
    """
    Identifies top-selling product categories by total sales.
    """
    result = df.groupBy("Category") \
        .agg(spark_sum("TotalSales").alias("CategorySales")) \
        .orderBy(desc("CategorySales"))
    print("🏆 Top-selling product categories calculated.")
    return result


def region_sales(df):
    """
    Calculates total sales per region or state.
    """
    result = df.groupBy(col("`ship-state`").alias("States")) \
        .agg(spark_sum("TotalSales").alias("RegionSales")) \
        .orderBy(desc("RegionSales"))
    print("🌍 Region-wise sales trends calculated.")
    return result


from pyspark.sql.functions import countDistinct, desc

def customer_preferences(df):
    result = df.groupBy("Category") \
        .agg(countDistinct("Order ID").alias("NumOrders")) \
        .orderBy(desc("NumOrders"))
    print("👥 Customer preferences analyzed (orders per category).")
    return result


