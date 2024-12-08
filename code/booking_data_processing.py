# Databricks notebook source
from pyspark.sql.functions import col, lit, current_timestamp, sum as _sum
from delta.tables import DeltaTable
from pydeequ.checks import Check, CheckLevel
from pydeequ.verification import VerificationSuite, VerificationResult
import os

# COMMAND ----------

print(os.environ['SPARK_VERSION'])

# COMMAND ----------

# Get job parameters from Databricks
# date_str = dbutils.widgets.get("current_date")
date_str = "2024-07-26"

# COMMAND ----------

dbutils.fs.ls("dbfs:/FileStore/bookings_data_processing/customers_daily_data/")

# COMMAND ----------

# Define file path based on date parameter
booking_data = f"dbfs:/FileStore/bookings_data_processing/bookings_daily_data/bookings_{date_str}.csv"
customer_data = f"dbfs:/FileStore/bookings_data_processing/customers_daily_data/customers_{date_str}.csv"

print(booking_data)
print(customer_data)

# COMMAND ----------

# Read booking data
booking_df = spark.read.format("csv").option("header","true").option("inferSchema","true").option("quote","\"")\
    .option("multiLine","true").load(booking_data)

booking_df.printSchema()
display(booking_df)

# COMMAND ----------

# Read customer data for SCD2 merge
customer_df = spark.read.format("csv").option("header","true").option("inferSchema","true")\
    .option("quote","\"").option("multiLine","true").load(customer_data)

customer_df.printSchema()
display(customer_df)

# COMMAND ----------

# Data quality checks on booking data
check_incremental = Check(spark, CheckLevel.Error, "Booking Data Check")\
    .hasSize(lambda x:x>0)\
    .isUnique("booking_id", hint="Booking ID is not unique")\
    .isComplete("customer_id")\
    .isComplete("amount")\
    .isNonNegative("amount")\
    .isNonNegative("quantity")\
    .isNonNegative("discount")

# COMMAND ----------

# Data quality checks on customer data
check_scd = Check(spark, CheckLevel.Error, "Customer Data Check")\
    .hasSize(lambda x:x>0)\
    .isUnique("customer_id")\
    .isComplete("customer_name")\
    .isComplete("customer_address")\
    .isComplete("phone_number")\
    .isComplete("email")

# COMMAND ----------

# Run the verfication suite
booking_dq_check = VerificationSuite(spark).onData(booking_df).addCheck(check_incremental).run()
customer_dq_check = VerificationSuite(spark).onData(customer_df).addCheck(check_scd).run()

# COMMAND ----------

booking_dq_check_df = VerificationResult.checkResultsAsDataFrame(spark,booking_dq_check)
display(booking_dq_check_df)

customer_dq_check_df = VerificationResult.checkResultsAsDataFrame(spark,customer_dq_check)
display(customer_dq_check_df)

# COMMAND ----------

if booking_dq_check.status != "Success":
    raise ValueError ("Data Quality Checks Failed for Booking data")

if customer_dq_check.status != "Success":
    raise ValueError ("Data Quality Checks Failed for Customer data")

# COMMAND ----------

# Add ingestion timestamp to booking data
booking_df_incremental = booking_df.withColumn("ingestion_time",current_timestamp())

# Join booking data with customer data
df_joined = booking_df_incremental.join(customer_df,"customer_id")

# Business transformation: calculate total cost after discount and filter
df_transformed = df_joined.withColumn("total_cost", col("amount") - col("discount"))\
                            .filter(col("quantity")>0)

# Group by and aggregate df_transformed
df_transformed_agg = df_transformed.groupBy("booking_type","customer_id")\
                        .agg(
                            _sum("total_cost").alias("total_amount_sum"),
                            _sum("quantity").alias("total_quantity_sum")
                        )

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE SCHEMA IF NOT EXISTS bookings;

# COMMAND ----------

# Check if the delta table exists
fact_table_path = "pyspark_playground.bookings.booking_fact"
fact_table_exists = spark._jsparkSession.catalog().tableExists(fact_table_path)

if fact_table_exists:
    # Read the existing fact table
    df_existing_fact = spark.read.format("delta").table(fact_table_path)

    # Combine the aggregated data with the existing fact table
    df_combined = df_existing_fact.unionByName(df_transformed_agg,allowMissingColumns=True)

    # Perform another group by and aggregation on the combined data
    df_final_agg = df_combined\
        .groupBy("booking_type","customer_id")\
            .agg(
                _sum("total_amount_sum").alias("total_amount_sum"),
                _sum("total_quantity_sum").alias("total_quantity_sum")
            )
else:
    # If the fact table doesn't exist, use the aggregated transformed data directly
    df_final_agg = df_transformed_agg

display(df_final_agg)

# COMMAND ----------

# Write final aggregated data to the delta table
df_final_agg.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(fact_table_path)

# COMMAND ----------

scd_table_path = "pyspark_playground.bookings.customer_scd"
scd_table_exists = spark._jsparkSession.catalog().tableExists(scd_table_path)

# Check if the customers table exists
if scd_table_exists:
    # Load the existing scd table
    scd_table = DeltaTable.forName(spark, scd_table_path)
    display(scd_table.toDF())

    # Perform SCD2 Merge operation
    scd_table.alias("scd")\
        .merge(source=customer_df.alias("updates"),
               condition="scd.customer_id = updates.customer_id and scd.valid_to = '9999-12-31'")\
        .whenMatchedUpdate(set={"valid_to":"updates.valid_from"}).execute()

    customer_df.write.format("delta").mode("append").saveAsTable(scd_table_path)

else:
    # If the SCD table doesn't exist, write the customer data as a new Delta table
    customer_df.write.format("delta").mode("overwrite").saveAsTable(scd_table_path)