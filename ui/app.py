# ui/app.py
import sys
import os
import tempfile
import streamlit as st
import pandas as pd
from pyspark.sql.functions import col

# Add project root
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

import src.config_env
from src.data_collection import load_data
from src.preprocessing import preprocess_data
from src.analysis import category_sales, top_selling_categories, region_sales, customer_preferences
from src.visualization import (
    plot_category_sales, plot_top_categories, plot_region_sales, plot_customer_preferences
)

# Page setup
st.set_page_config(page_title="E-Commerce Sales Dashboard", page_icon="📊", layout="wide")
st.title("🛒 E-Commerce Sales Analytics Dashboard")

# Initialize session
if "spark" not in st.session_state:
    st.session_state.spark = None
if "df_clean" not in st.session_state:
    st.session_state.df_clean = None
if "spark_closed" not in st.session_state:
    st.session_state.spark_closed = False

# Sidebar Upload
st.sidebar.header("📂 Upload Dataset")
uploaded_file = st.sidebar.file_uploader("Upload your sales CSV file", type=["csv"])

# Show info if no file
if not uploaded_file and not st.session_state.spark:
    st.sidebar.info("Please upload dataset to continue. Required Columns: - Order ID,Date,Category,Qty,\nAmount,Ship-state ")
    st.stop()

# Load data
def load_and_preprocess(uploaded_file):
    with tempfile.NamedTemporaryFile(delete=False, suffix=".csv") as tmp:
        tmp.write(uploaded_file.getbuffer())
        temp_path = tmp.name
    df, spark = load_data(temp_path)
    df_clean = preprocess_data(df)
    st.session_state.spark = spark
    st.session_state.df_clean = df_clean
    st.session_state.spark_closed = False
    return df_clean, spark

# ✅ Load dataset only if Spark is not closed (fresh upload)
if uploaded_file and not st.session_state.spark and not st.session_state.spark_closed:
    with st.spinner("Processing dataset ⏳"):
        df_clean, spark = load_and_preprocess(uploaded_file)
    st.sidebar.success("✅ File Processed Successfully!")

# Access session data
df_clean = st.session_state.df_clean
spark = st.session_state.spark

# Sidebar Menu
st.sidebar.title("📊 Dashboard Menu")
menu = st.sidebar.radio("Select Analysis", [
    "1️⃣ Total Sales by Category",
    "2️⃣ Top Selling Categories",
    "3️⃣ Region-wise Sales Trends",
    "4️⃣ Customer Preferences",
    "5️⃣ Predict Future Sales (Forecasting)",
    "❌ Exit"
])

# ✅ Block all actions if Spark is closed (except Exit)
if st.session_state.spark_closed and not menu.startswith("❌"):
    st.warning("⚠️ Spark session has been closed. Please upload a new dataset!")
    st.stop()



# ---------------- 1️⃣ TOTAL SALES BY CATEGORY ----------------
if menu.startswith("1️⃣") and spark:
    st.title("📊 Sales by Category")
    cat_sales_df = category_sales(df_clean)
    st.dataframe(cat_sales_df.toPandas())
    chart_type = st.radio("📊 Choose Chart Type:", ["Bar", "Pie"], horizontal=True)
    fig = plot_category_sales(cat_sales_df, chart_type)
    if chart_type == "Pie":
        st.plotly_chart(fig, use_container_width=True)
    else:
        st.pyplot(fig)

    # ✅ Insights
    top_cat = cat_sales_df.orderBy(col("CategorySales").desc()).first()
    low_cat = cat_sales_df.orderBy(col("CategorySales")).first()

    st.markdown(f"""
    ### 📝 Insights
    - 🏆 Highest selling category: **{top_cat['Category']}**
    - 📉 Lowest selling category: **{low_cat['Category']}**
    - 💡 Focus marketing on top performers & revive low demand categories  
    """)

# ---------------- 2️⃣ TOP SELLING CATEGORIES ----------------
elif menu.startswith("2️⃣") and spark:
    st.title("🏆 Top Categories")
    top_cat_df = top_selling_categories(df_clean)
    st.dataframe(top_cat_df.toPandas())
    st.pyplot(plot_top_categories(top_cat_df, top_n=5))

    # ✅ Insights
    best = top_cat_df.orderBy(col("CategorySales").desc()).first()
    st.markdown(f"""
    ### 📝 Insights
    - ⭐ **{best['Category']}** leads in total orders!
    - 📌 Customer demand is heavily trend-driven  
    """)

# ---------------- 3️⃣ REGION SALES ----------------
elif menu.startswith("3️⃣") and spark:
    st.title("🌍 Region-wise Trends")
    region_sales_df = region_sales(df_clean)
    st.dataframe(region_sales_df.toPandas())
    from src.visualization import plot_region_sales

    st.plotly_chart(plot_region_sales(region_sales_df, top_n=10), use_container_width=True)

    # ✅ Insights
    top_region = region_sales_df.orderBy(col("RegionSales").desc()).first()
    bottom_region = region_sales_df.orderBy(col("RegionSales")).first()
    st.markdown(f"""
    ### 🌍 Regional Insights
    - 🥇 Top region: **{top_region['States']}**
    - ⚠️ Low performing region: **{bottom_region['States']}**
    - 🚚 Target better logistics & ads to improve low performing areas  
    """)

# ---------------- 4️⃣ CUSTOMER PREFERENCES ----------------
elif menu.startswith("4️⃣") and spark:
    st.title("👥 Customer Preferences")
    cust_pref_df = customer_preferences(df_clean)
    st.dataframe(cust_pref_df.toPandas())
    st.pyplot(plot_customer_preferences(cust_pref_df))

    # ✅ Insights
    top_pref = cust_pref_df.orderBy(col("NumOrders").desc()).first()
    st.markdown(f"""
    ### 👥 Preference Insights
    - ✅ Highest demand for **{top_pref['Category']}**
    - 🛍️ Inventory & offers should focus here for maximum profit  
    """)

# ---------------- 5️⃣ FORECASTING ----------------
elif menu.startswith("5️⃣") and spark:
    from src.forecasting import train_random_forest, forecast_future
    from pyspark.sql.functions import sum as spark_sum

    st.title("📈 Future Sales Forecast")

    category_list = df_clean.select("Category").distinct().toPandas()["Category"].tolist()
    selected_category = st.selectbox("Category Filter", ["All"] + category_list)
    forecast_days = st.slider("Forecast Duration", 7, 90, 30)

    with st.spinner("Generating Forecast..."):
        df_filtered = df_clean if selected_category == "All" else df_clean.filter(df_clean.Category == selected_category)

        df_daily = df_filtered.groupBy("Date").agg(spark_sum("TotalSales").alias("TotalSales")).orderBy("Date")

        pdf_daily = df_daily.toPandas().sort_values("Date")
        pdf_daily = pdf_daily.set_index("Date").asfreq("D", fill_value=0).reset_index()

        pdf_daily["day"] = pdf_daily["Date"].dt.day
        pdf_daily["month"] = pdf_daily["Date"].dt.month
        pdf_daily["week"] = pdf_daily["Date"].dt.isocalendar().week
        pdf_daily["lag_1"] = pdf_daily["TotalSales"].shift(1)
        pdf_daily["lag_7"] = pdf_daily["TotalSales"].shift(7)
        pdf_daily = pdf_daily.dropna()

        train_pdf = pdf_daily.rename(columns={"TotalSales": "Sales"})
        model = train_random_forest(train_pdf)
        future_forecast = forecast_future(model, train_pdf[["Date", "Sales"]], periods=forecast_days)

    train_pdf["Type"] = "Historical"
    future_forecast["Type"] = "Forecast"
    final_pd = pd.concat([train_pdf, future_forecast], ignore_index=True)

    st.subheader("📊 Actual vs Forecast")
    chart_data = final_pd.pivot(index="Date", columns="Type", values="Sales")
    from src.visualization import plot_forecast_area

    fig = plot_forecast_area(train_pdf, future_forecast)
    st.plotly_chart(fig, use_container_width=True)

    # ✅ Insights
    last_hist = train_pdf["Sales"].iloc[-1]
    max_future = future_forecast["Sales"].max()
    trend = "⬆ increasing" if max_future > last_hist else "⬇ decreasing"
    st.markdown(f"""
    ### 🔮 Forecast Insights
    - 📈 Sales trend is **{trend}**
    - 🔝 Expected peak future sales: **{max_future:.2f}**
    - ✅ Helps in stock planning & business forecasting  
    """)

# ---------------- EXIT ----------------
# ❌ Exit (final close)

elif menu.startswith("❌"):
    st.title("👋 Exit Dashboard")

    if st.button("🛑 Close Spark Session"):
        if st.session_state.spark:
            st.session_state.spark.stop()

        st.session_state.spark = None
        st.session_state.df_clean = None
        st.session_state.spark_closed = True  # ✅ lock functionalities

        st.success("✅ Spark session closed! Upload a new dataset to restart.")
        st.rerun()  # ✅ refresh UI to hide everything else



