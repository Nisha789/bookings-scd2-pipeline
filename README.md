# Bookings Data Processing with SCD2 Merge

This Databricks notebook project automates the processing and merging of customer and booking data using Slowly Changing Dimension Type 2 (SCD2). It handles data quality checks, aggregation, and Delta table management for efficient data processing and analysis.

## Features

- **Automated Databricks workflow**: Fully automated workflow to handle data processing tasks.
- **Data Quality Checks**: Ensures data integrity with comprehensive checks on booking and customer data.
- **SCD2 Merge**: Manages historical data changes and maintains valid-to/valid-from records for customer data.
- **Delta Table Management**: Aggregates and stores consolidated booking information in a Delta table.

## Prerequisites

- Databricks workspace with appropriate permissions.
- Access to DBFS GCP Buckets for data files.
- **Libraries:**
    PyDeequ and Py4J are required for performing data quality checks and are available via pip.
    Maven: May be required for managing dependencies in Databricks environments.

## Usage

1. **Upload Data Files**: Place the `customer_data` and `booking_data` files in the appropriate DBFS GCP Buckets.
2. **Execute Notebook**: Run the Databricks notebook associated with this project. It will:
   - Read and validate data files.
   - Perform data quality checks.
   - Merge data using SCD2.
   - Store the final aggregated data in a Delta table.

## Notebooks Included

- **`booking_data_processing`**: Handles data reading, quality checks, transformation, and Delta table creation.
- **`customer_data_processing`**: Manages the Slowly Changing Dimension Type 2 merge for customer data.

## Data Paths

- `booking_data`: Path to the booking data file in DBFS GCP Bucket.
- `customer_data`: Path to the customer data file in DBFS GCP Bucket.

## Automated Workflow

This project is automated using a Databricks workflow that triggers these notebooks sequentially to ensure a streamlined data processing pipeline.

## Contributions

Contributions are welcome! If you'd like to contribute, please fork the repository and submit a pull request with your enhancements.

## License

This project is licensed under the [MIT License](LICENSE).

## Acknowledgments

We would like to acknowledge the following:

- **Databricks**: For providing a powerful platform for automating data workflows and data processing.
- **PySpark and Delta Lake**: For efficient data processing and management in Spark.
- **Google Cloud Platform (GCP)**: For offering scalable storage solutions in the form of DBFS GCP Buckets, which are essential for handling large data files.
