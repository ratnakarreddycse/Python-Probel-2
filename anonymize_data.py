import os

# Set the correct Python path
os.environ['PYSPARK_PYTHON'] = 'C:\\Users\\Ratnakar Reddy\\AppData\\Local\\Programs\\Python\\Python312\\python.exe'
os.environ['PYSPARK_DRIVER_PYTHON'] = 'C:\\Users\\Ratnakar Reddy\\AppData\\Local\\Programs\\Python\\Python312\\python.exe'



from pyspark.sql import SparkSession
from pyspark.sql.functions import udf
from faker import Faker
from pyspark.sql.types import StringType

# Function to generate random first name
def generate_first_name():
    fake = Faker()
    return fake.first_name()

# Function to generate random last name
def generate_last_name():
    fake = Faker()
    return fake.last_name()

# Function to generate random address
def generate_address():
    fake = Faker()
    return fake.address()

# Main function to anonymize the CSV
def anonymize_csv(input_file, output_file):
    spark = SparkSession.builder.appName('DataAnonymization').getOrCreate()
    
    # Reduce log level
    spark.sparkContext.setLogLevel("ERROR")

    # Register UDFs
    first_name_udf = udf(generate_first_name, StringType())
    last_name_udf = udf(generate_last_name, StringType())
    address_udf = udf(generate_address, StringType())

    # Read CSV into DataFrame
    df = spark.read.csv(input_file, header=True, inferSchema=True)

    # Apply UDFs to anonymize columns
    anonymized_df = df.withColumn("first_name", first_name_udf()) \
                      .withColumn("last_name", last_name_udf()) \
                      .withColumn("address", address_udf())

    # Write the anonymized data back to a CSV file
    anonymized_df.write.csv(output_file, header=True, mode='overwrite')

    spark.stop()

if __name__ == "__main__":
    input_file = 'large_dataset.csv'
    output_file = 'anonymized_output.csv'
    
    anonymize_csv(input_file, output_file)
    print("Data anonymized and saved.")



