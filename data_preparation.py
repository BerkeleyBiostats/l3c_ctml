from src.STEP1_feature import *
import os
from pyspark.sql import SparkSession

def main():
    spark = SparkSession.builder \
        .master("local[1]") \
        .appName("l3c_ctml") \
        .getOrCreate()

    # Read CSV file into table
    data_path = "synthetic_data"

    for path, subdirs, files in os.walk(data_path):
        for name in files:
            if "csv" in name and name[0]!=".":
                full_file = (os.path.join(path, name))
                table_name = name[:-4]
                print(full_file)
                # read and create pyspark tables from them
                print("reading")
                spark.read.option("header", True) \
                    .csv(full_file) \
                    .createOrReplaceTempView(table_name)


    Collect_the_Cohort = spark.sql(sql_statement_00())





if __name__ == "__main__":
    main()