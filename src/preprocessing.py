# src/preprocessing.py
from pyspark.sql.functions import col, coalesce, to_date, expr, lit
import streamlit as st

def preprocess_data(df):
    """
    Clean and validate uploaded sales data for analysis:
    ✅ Checks required columns before use
    ✅ Handles missing or mismatched column names
    ✅ Converts date formats
    ✅ Removes invalid rows and adds TotalSales column
    """

    # 1️⃣ Standard lowercase names we will enforce after renaming
    required_columns = ["order id", "date", "category", "qty", "amount"]
    optional_columns = ["style", "ship-state"]

    # 2️⃣ Normalize column names
    df = df.toDF(*[c.strip().lower() for c in df.columns])

    # 3️⃣ Map potential variations
    rename_map = {
        "orderid": "order id",
        "order id": "order id",
        "id": "order id",
        "date": "date",
        "category": "category",
        "productcategory": "category",
        "qty": "qty",
        "quantity": "qty",
        "amount": "amount",
        "price": "amount",
        "style": "style",
        "shipstate": "ship-state",
        "state": "ship-state"
    }

    # 4️⃣ Rename matching columns
    for c in df.columns:
        clean = c.replace(" ", "").lower()
        if clean in rename_map:
            df = df.withColumnRenamed(c, rename_map[clean])

    # 5️⃣ Check missing
    cols = df.columns
    missing_required = [c for c in required_columns if c not in cols]
    missing_optional = [c for c in optional_columns if c not in cols]

    if missing_required:
        st.error(f"❌ Missing required columns: {', '.join(missing_required)}")
        st.stop()

    # 6️⃣ Auto-add missing optional
    for c in missing_optional:
        df = df.withColumn(c, lit(None))

    # 7️⃣ Remove null rows
    df = df.dropna(subset=required_columns)

    # 8️⃣ Convert date column correctly
    df = df.withColumn(
        "date",
        coalesce(
            to_date(expr("try_to_timestamp(date, 'yyyy-MM-dd')")),
            to_date(expr("try_to_timestamp(date, 'MM-dd-yy')")),
            to_date(expr("try_to_timestamp(date, 'dd-MM-yy')")),
            to_date(expr("try_to_timestamp(date, 'MM/dd/yy')")),
            to_date(expr("try_to_timestamp(date, 'dd/MM/yy')"))
        )
    )

    df = df.filter(col("date").isNotNull())

    # 9️⃣ Ensure valid numbers
    df = df.filter((col("qty") > 0) & (col("amount") > 0))

    # 🔟 Add TotalSales
    df = df.withColumn("totalsales", col("amount"))

    st.success("✅ Data preprocessing completed successfully!")
    st.write(f"**Total Valid Records:** {df.count()}")

    return df
