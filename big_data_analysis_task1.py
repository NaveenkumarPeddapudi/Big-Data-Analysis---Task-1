# big_data_analysis_task1.py
# Task-1: Big Data Analysis using PySpark + Matplotlib
# Author: Peddapudi Naveenkumar

import pandas as pd
import matplotlib.pyplot as plt
import logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, count, desc, round as spark_round
from pathlib import Path


# ========== Config ==========
DATA_DIR = Path(r"C:\Users\pedda\OneDrive\Desktop\ESAIP CLG SUBJECTS\Internship\INTERNSHIP\Task-1")
INPUT_CSV = DATA_DIR / "orders_data.csv"
SUMMARY_CSV = DATA_DIR / "product_summary_output.csv"
CHART_PATH = DATA_DIR / "total_orders_per_product.png"


# ========== Logging Setup ==========
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')


# ========== PySpark Analysis ==========
def run_spark_analysis(input_csv: Path) -> pd.DataFrame:
    logging.info("Starting Spark session...")
    spark = SparkSession.builder.appName("Big Data Analysis - Task 1").getOrCreate()

    logging.info(f"Reading CSV file: {input_csv}")
    df_spark = spark.read.csv(str(input_csv), header=True, inferSchema=True)
    df_spark = df_spark.withColumn("TotalPrice", col("Quantity") * col("UnitPrice"))

    logging.info("Showing sample data:")
    df_spark.show(5)

    logging.info("Generating product summary...")
    summary_df = df_spark.groupBy("Product").agg(
        count("*").alias("TotalOrders"),
        spark_round(avg("Quantity"), 1).alias("AvgQuantity"),
        spark_round(avg("UnitPrice"), 1).alias("AvgPrice"),
        spark_round(avg("TotalPrice"), 1).alias("AvgTotalSale")
    ).orderBy(desc("TotalOrders"))

    summary_df.show()

    # Convert to pandas for exporting and charting
    summary_pd = summary_df.toPandas()

    spark.stop()
    logging.info("Spark session stopped.")
    return summary_pd


# ========== Visualization ==========
def create_bar_chart(df: pd.DataFrame, output_path: Path):
    logging.info("Creating bar chart...")

    plt.figure(figsize=(10, 6))
    bars = plt.bar(df['Product'], df['TotalOrders'], color='orange')

    plt.title('Total Orders per Product')
    plt.xlabel('Product')
    plt.ylabel('Total Orders')
    plt.xticks(rotation=45, ha='right')
    plt.tight_layout()

    # Add total orders as labels on bars
    for bar in bars:
        height = bar.get_height()
        plt.text(bar.get_x() + bar.get_width() / 2, height + 1, f'{int(height)}', ha='center', va='bottom')

    plt.savefig(str(output_path))
    plt.show()
    logging.info(f"Chart saved to {output_path}")


# ========== Main ==========
def main():
    try:
        logging.info("Big Data Analysis Task Started.")
        
        # Run PySpark analysis and export CSV
        summary_df = run_spark_analysis(INPUT_CSV)
        summary_df.to_csv(SUMMARY_CSV, index=False)
        logging.info(f"Summary data saved to: {SUMMARY_CSV}")

        # Load data again with pandas for charting (optional)
        df_pd = pd.read_csv(INPUT_CSV)
        df_pd['TotalPrice'] = df_pd['Quantity'] * df_pd['UnitPrice']
        chart_data = df_pd.groupby('Product').agg(TotalOrders=('OrderID', 'count')).reset_index()

        # Create and save bar chart
        create_bar_chart(chart_data, CHART_PATH)

        logging.info("Big Data Analysis Task Completed Successfully.")

    except Exception as e:
        logging.error(f"Error occurred: {e}", exc_info=True)


if __name__ == "__main__":
    main()
