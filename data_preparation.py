from src.STEP1_feature import a_cohort as a, b_medication_sql as b
import os
from pyspark.sql import SparkSession
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType,StructField, StringType

def main():
    spark = SparkSession.builder \
        .master("local[1]") \
        .appName("l3c_ctml") \
        .getOrCreate()

    # Read CSV file into table
    data_path = "synthetic_data/training"

    schema = StructType([
        StructField('table', StringType(), True),
        StructField('field', StringType(), True),
        StructField('type', StringType(), True)
    ])

    all_fields = spark.createDataFrame([], schema)

    for path, subdirs, files in os.walk(data_path):
        for name in files:
            if "csv" in name and name[0]!=".":
                # table_fields = [(table_name, f.name, str(f.dataType)) for f in datafile.schema.fields]
                full_file = (os.path.join(path, name))
                df = spark.read.csv(os.path.join(path, name), header=True, inferSchema=True)
                var_name = name[:-4]
                print(full_file)
                globals()[var_name] = df

    # 1_cohort_and_features

    covid_pasc_index_dates = a.sql_statement_01(long_covid_silver_standard)
    Collect_the_Cohort = a.sql_statement_00(covid_pasc_index_dates, person)




if __name__ == "__main__":
    main()