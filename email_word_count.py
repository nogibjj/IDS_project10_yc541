from pyspark.sql import SparkSession

# Initialize Spark session
spark = SparkSession.builder \
    .appName("Email Data Analysis") \
    .getOrCreate()

# Read the dataset
df = spark.read.csv("emails_dataset.csv", header=True, inferSchema=True)

# Select specific columns for analysis
# Assuming you want to select the columns as seen in your CSV file
df_selected = df.select(
    "`Email No.`", "the", "to", "ect", "and", "for", "of", "a", "you", "hou", "in", "on", "is", "this"
)

# Register the DataFrame as a SQL temporary view
df_selected.createOrReplaceTempView("email_data")

# Use SQL query to find the most common words counts across all emails
# This example will sum up all occurrences of the word 'the'
most_common_word_count = spark.sql("""
SELECT SUM(`the`) as total_the, SUM(`to`) as total_to, SUM(`ect`) as total_ect
FROM email_data
""")

most_common_word_count.show()

# Example of data transformation: Convert all the word count columns to integer type
# This step may not be necessary if your columns were properly inferred as integers,
# but it is included here for completeness.
for column_name in df_selected.columns:
    if column_name != "Email No.":
        df_selected = df_selected.withColumn(column_name, df_selected[column_name].cast("integer"))

# Save the transformed DataFrame to a new CSV file
df_selected.write.csv('transformed_emails_dataset.csv', header=True)

# Stop the Spark session
spark.stop()
