from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.types import IntegerType

# Initialize Spark session
spark = SparkSession.builder \
    .appName("Email Data Analysis") \
    .getOrCreate()

# Read the dataset
df = spark.read.csv("path_to_your_csv.csv", header=True, inferSchema=True)

# Assuming the column names in the dataset are as follows
columns = [
    "Email_No",
    "Email_Name",
    "Most_common_word_1",
    "Most_common_word_2",
    # ... and so on
    "Most_common_word_9",
    "Unique_values",
    # ... and other statistical data
]

# Select specific columns for analysis
df_selected = df.select(
    "Email_No",
    "Email_Name",
    "Most_common_word_1",
    "Most_common_word_2",
    "Most_common_word_3"
)

# Register the DataFrame as a SQL temporary view
df_selected.createOrReplaceTempView("email_data")

# Use SQL query to find the most common words
most_common_words = spark.sql("""
SELECT
    Most_common_word_1 AS Word,
    COUNT(Most_common_word_1) AS Word_Count
FROM email_data
GROUP BY Most_common_word_1
ORDER BY Word_Count DESC
LIMIT 10
""")

most_common_words.show()

# Example of data transformation: Convert all the common word count columns to integer type
for i in range(1, 10):
    column_name = f"Most_common_word_{i}"
    df = df.withColumn(column_name, col(column_name).cast(IntegerType()))

# Save the transformed DataFrame to a new CSV file
df.write.csv('path_to_output_csv.csv', header=True)

# Stop the Spark session
spark.stop()
