import pandas as pd
from pyspark.sql.functions import col, dayofmonth, month, weekofyear, lag
from pyspark.sql.window import Window
from pyspark.ml.feature import VectorAssembler
from sklearn.ensemble import RandomForestRegressor

# ------------------------------
# 📌 Prepare Spark Data for ML
# ------------------------------
def prepare_ml_data(df):
    df = df.withColumn("day", dayofmonth(col("Date")))
    df = df.withColumn("month", month(col("Date")))
    df = df.withColumn("week", weekofyear(col("Date")))

    window = Window.orderBy("Date")
    df = df.withColumn("lag_1", lag(col("TotalSales"), 1).over(window))
    df = df.withColumn("lag_7", lag(col("TotalSales"), 7).over(window))

    df = df.dropna()
    return df

# ------------------------------
# 🎯 Train Random Forest in Pandas
# ------------------------------
def train_random_forest(pdf):
    X = pdf[["day", "month", "week", "lag_1", "lag_7"]]
    y = pdf["Sales"]

    model = RandomForestRegressor(
        n_estimators=50,
        max_depth=10,
        random_state=42
    )
    model.fit(X, y)
    return model

# ------------------------------
# 🔮 Forecast Future using Pandas
# ------------------------------
def forecast_future(model, pdf, periods=30):
    pdf = pdf.copy().sort_values("Date")

    future_rows = []
    last_date = pdf["Date"].max()

    for _ in range(periods):
        future_date = last_date + pd.Timedelta(days=1)

        pdf["day"] = pdf["Date"].dt.day
        pdf["month"] = pdf["Date"].dt.month
        pdf["week"] = pdf["Date"].dt.isocalendar().week

        lag_1 = pdf["Sales"].iloc[-1]
        lag_7 = pdf["Sales"].iloc[-7] if len(pdf) >= 7 else lag_1

        features = [[
            future_date.day,
            future_date.month,
            future_date.isocalendar().week,
            lag_1,
            lag_7
        ]]

        pred = model.predict(features)[0]
        pred = max(pred, 0)

        row = [{"Date": future_date, "Sales": pred}]
        future_rows.append(row[0])

        pdf.loc[len(pdf)] = [future_date, pred, None, None, None]  # dummy for maintaining structure

        last_date = future_date

    return pd.DataFrame(future_rows)
