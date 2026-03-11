import os

# ✅ Java setup (already correct)
os.environ["JAVA_HOME"] = r"C:\Progra~1\Eclipse Adoptium\jdk-17.0.11.9-hotspot"
os.environ["PATH"] = os.environ["JAVA_HOME"] + r"\bin;" + os.environ["PATH"]

# ✅ Prevent Spark Python worker failure
os.environ["PYSPARK_PYTHON"] = r"C:\ecommerce_sales_analysis\.venv\Scripts\python.exe"
os.environ["PYSPARK_DRIVER_PYTHON"] = r"C:\ecommerce_sales_analysis\.venv\Scripts\python.exe"

# ✅ Fix Arrow conversion crash for RandomForest
os.environ["ARROW_PRE_0_15_IPC_FORMAT"] = "1"

# ✅ Override TEMP to avoid path issues
os.environ["TMP"] = r"C:\Temp"
os.environ["TEMP"] = r"C:\Temp"
if not os.path.exists(r"C:\Temp"):
    os.makedirs(r"C:\Temp")
