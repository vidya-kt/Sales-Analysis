# E-Commerce Sales Analysis and Forecasting System

## Overview

The **E-Commerce Sales Analysis and Forecasting System** is a data-driven analytics platform designed to extract meaningful insights from large-scale e-commerce sales datasets. The system performs automated data preprocessing, sales analysis, and future sales prediction using machine learning techniques.

It integrates **PySpark for scalable data processing**, **Prophet for time-series forecasting**, and **Streamlit for an interactive dashboard**. The application enables businesses and analysts to visualize category-wise sales, regional trends, customer preferences, and predicted future sales in an easy-to-use web interface.

---

## Features

* Automated **data preprocessing and cleaning**
* **Category-wise sales analysis**
* **Top-selling product identification**
* **Region-wise sales trend analysis**
* **Customer purchase preference insights**
* **Future sales forecasting**
* Interactive **Streamlit dashboard**
* Visualization using charts and graphs

---

## Technologies Used

* Python
* PySpark
* Prophet (Time-Series Forecasting)
* Streamlit
* Pandas
* NumPy
* Matplotlib
* Seaborn

---

## Project Structure

```
ecommerce_sales_analysis
│
├── data/
│
├── src/
│   ├── analysis.py
│   ├── config_env.py
│   ├── data_collection.py
│   ├── forecasting.py
│   ├── preprocessing.py
│   └── visualization.py
│
├── ui/
│   └── app.py
│
├── requirements.txt
└── .gitignore
```

---

## Installation

1. Clone the repository

```
git clone https://github.com/yourusername/ecommerce_sales_analysis.git
cd ecommerce_sales_analysis
```

2. Create virtual environment

```
python -m venv .venv
```

3. Activate environment

Windows

```
.venv\Scripts\activate
```

4. Install dependencies

```
pip install -r requirements.txt
```

---

## Running the Application

Run the Streamlit dashboard:

```
streamlit run ui/app.py
```

Then open the browser at:

```
http://localhost:8501
```

Upload a CSV dataset and explore the analysis and forecasting modules.

---

## Key Insights Generated

The system provides insights such as:

* Highest and lowest selling product categories
* Region-wise sales performance
* Customer purchasing preferences
* Forecasted future sales trends

These insights help businesses make **data-driven decisions for inventory planning, marketing strategies, and sales optimization**.

---

## Author

**Vidya K T**
MCA Student – Bangalore Institute of Technology

Developed as part of an academic project for the MCA program.
