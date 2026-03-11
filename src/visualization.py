# src/visualization.py

import matplotlib.pyplot as plt
import seaborn as sns
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go

# ✅ Apply a clean, professional theme globally
sns.set_theme(style="whitegrid", palette="crest")

# -------------------------------------
# 📊 CATEGORY SALES (Bar or Pie Toggle)
# -------------------------------------
def plot_category_sales(df_spark, chart_type="Bar"):
    """
    Plot total sales per product category (Bar or Pie view).
    """
    df = df_spark.toPandas().sort_values("CategorySales", ascending=False)

    if chart_type == "Pie":
        # 🥧 Interactive Pie Chart
        fig = px.pie(
            df,
            values="CategorySales",
            names="Category",
            title="💰 Sales Share by Category",
            color_discrete_sequence=px.colors.sequential.Teal
        )
        fig.update_traces(textinfo="percent+value+label", textfont_size=13, pull=[0.05]*len(df))
        return fig

    else:
        # 📊 Bar Chart (default)
        fig, ax = plt.subplots(figsize=(10, 6))
        bars = sns.barplot(x="Category", y="CategorySales", data=df, ax=ax)
        ax.set_xticklabels(ax.get_xticklabels(), rotation=45, ha="right", fontsize=10)
        ax.set_title("💰 Total Sales per Product Category", fontsize=14, weight="bold")
        ax.set_xlabel("Product Category", fontsize=12)
        ax.set_ylabel("Total Sales (Revenue)", fontsize=12)
        ax.grid(True, linestyle="--", alpha=0.4)

        for p in bars.patches:
            ax.text(p.get_x() + p.get_width()/2, p.get_height(),
                    f"{p.get_height():,.0f}", ha='center', va='bottom', fontsize=9, color="#333")

        fig.tight_layout()
        return fig


# -------------------------------------
# 🏆 TOP SELLING CATEGORIES
# -------------------------------------
def plot_top_categories(df_spark, top_n=5):
    """
    Plot top N product categories by total sales (enhanced).
    """
    df = df_spark.toPandas().nlargest(top_n, "CategorySales")
    fig, ax = plt.subplots(figsize=(10, 6))

    bars = sns.barplot(x="Category", y="CategorySales", data=df, ax=ax)
    ax.set_title(f"🏆 Top {top_n} Product Categories by Sales", fontsize=14, weight="bold")
    ax.set_xlabel("Category", fontsize=12)
    ax.set_ylabel("Sales (Revenue)", fontsize=12)
    ax.set_xticklabels(ax.get_xticklabels(), rotation=45, ha="right")
    ax.grid(True, linestyle="--", alpha=0.4)

    for p in bars.patches:
        ax.text(p.get_x() + p.get_width()/2, p.get_height(),
                f"{p.get_height():,.0f}", ha='center', va='bottom', fontsize=9)

    fig.tight_layout()
    return fig


import plotly.express as px
import pandas as pd

def plot_region_sales(df_spark, top_n=10):
    """
    Plot region-wise sales (state-wise) as an interactive pie chart.
    """
    # ✅ Convert from Spark to Pandas
    df = df_spark.toPandas()

    # ✅ Handle column naming variations
    if "States" in df.columns:
        state_col = "States"
    elif "ship-state" in df.columns:
        df.rename(columns={"ship-state": "States"}, inplace=True)
        state_col = "States"
    else:
        raise KeyError("❌ Column 'States' or 'ship-state' not found in dataset.")

    # ✅ Check for RegionSales column
    if "RegionSales" not in df.columns:
        raise KeyError("❌ Column 'RegionSales' not found. Please check analysis.py output.")

    # ✅ Keep only top N states
    df = df.nlargest(top_n, "RegionSales")

    # ✅ Create interactive Pie Chart
    fig = px.pie(
        df,
        values="RegionSales",
        names=state_col,
        title=f"🌍 Top {top_n} Regions by Total Sales (Pie Chart)",
        color_discrete_sequence=px.colors.sequential.Teal
    )

    fig.update_traces(
        textinfo="percent+label",
        pull=[0.05] * len(df),
        hovertemplate="<b>%{label}</b><br>Sales: ₹%{value:,.0f}<br>%{percent}",
        textfont_size=12
    )

    fig.update_layout(
        showlegend=True,
        legend_title_text="States",
        template="plotly_white",
        margin=dict(l=40, r=40, t=60, b=40)
    )

    return fig

# -------------------------------------
# 👥 CUSTOMER PREFERENCES
# -------------------------------------
def plot_customer_preferences(df_spark):
    """
    Plot number of orders per category (enhanced).
    """
    df = df_spark.toPandas().sort_values("NumOrders", ascending=False)
    fig, ax = plt.subplots(figsize=(10, 6))

    bars = sns.barplot(x="Category", y="NumOrders", data=df, ax=ax)
    ax.set_title("👥 Customer Preferences by Category", fontsize=14, weight="bold")
    ax.set_xlabel("Category", fontsize=12)
    ax.set_ylabel("Number of Orders", fontsize=12)
    ax.set_xticklabels(ax.get_xticklabels(), rotation=45, ha="right")
    ax.grid(True, linestyle="--", alpha=0.4)

    for p in bars.patches:
        ax.text(p.get_x() + p.get_width()/2, p.get_height(),
                f"{p.get_height():,.0f}", ha='center', va='bottom', fontsize=9)

    fig.tight_layout()
    return fig


# -------------------------------------
# 🔮 FORECAST (Area Chart + Confidence Band)
# -------------------------------------
import plotly.graph_objects as go

def plot_forecast_area(train_df, forecast_df):
    """
    Plot area chart with interactive confidence band and hover values.
    """
    fig = go.Figure()

    # ✅ Historical line
    fig.add_trace(go.Scatter(
        x=train_df["Date"],
        y=train_df["Sales"],
        mode="lines",
        name="Historical Sales",
        line=dict(color="royalblue", width=2),
        fill='tozeroy',
        opacity=0.5,
        hovertemplate="📅 %{x|%b %d, %Y}<br>💰 Sales: ₹%{y:,.0f}<extra></extra>"
    ))

    # ✅ Forecast line
    fig.add_trace(go.Scatter(
        x=forecast_df["Date"],
        y=forecast_df["Sales"],
        mode="lines+markers",
        name="Forecasted Sales",
        line=dict(color="orange", width=2, dash="dot"),
        marker=dict(size=4),
        hovertemplate="📅 %{x|%b %d, %Y}<br>🔮 Forecast: ₹%{y:,.0f}<extra></extra>"
    ))

    # ✅ Confidence band (±5%)
    forecast_df["upper"] = forecast_df["Sales"] * 1.05
    forecast_df["lower"] = forecast_df["Sales"] * 0.95

    # 🟢 Add shaded confidence region with hover info
    fig.add_trace(go.Scatter(
        x=list(forecast_df["Date"]) + list(forecast_df["Date"])[::-1],
        y=list(forecast_df["upper"]) + list(forecast_df["lower"])[::-1],
        fill='toself',
        fillcolor='rgba(255,165,0,0.2)',
        line=dict(color='rgba(255,255,255,0)'),
        hoveron='fills',  # ✅ Enable hover for shaded area
        name="Confidence Band (±5%)",
        hovertemplate=(
            "📅 %{x|%b %d, %Y}<br>"
            "🔼 Upper: ₹%{y:,.0f}<br>"
            "🔽 Lower: ₹%{customdata:,.0f}<extra></extra>"
        ),
        customdata=list(forecast_df["lower"]) + list(forecast_df["upper"])[::-1]
    ))

    # ✅ Layout
    fig.update_layout(
        title="📈 Sales Forecast (Area Chart with Confidence Band)",
        xaxis_title="Date",
        yaxis_title="Sales (Revenue ₹)",
        legend_title="Legend",
        template="plotly_white",
        hovermode="x unified",
        margin=dict(l=40, r=40, t=60, b=40)
    )

    return fig
