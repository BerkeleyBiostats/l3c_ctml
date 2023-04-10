from src.STEP1_feature import a_cohort as a
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
    cohort = a.sql_statement_00(covid_pasc_index_dates, person)
    long_covid_patients = a.sql_statement_08(covid_pasc_index_dates)

    hosp_cases = a.sql_statement_04(cohort, condition_occurrence, microvisits_to_macrovisits)

    hosp_and_non = a.sql_statement_06(cohort, hosp_cases)

    Feature_Table_Builder_v0 = a.sql_statement_03(covid_pasc_index_dates, hosp_and_non, microvisits_to_macrovisits)
    icu_visits = a.sql_statement_05(microvisits_to_macrovisits, concept) # empty for some reason
    inpatient_visits = a.sql_statement_07(microvisits_to_macrovisits, concept)

    tot_icu_days_calc = a.sql_statement_09(Feature_Table_Builder_v0, icu_visits)
    tot_ip_days_calc = a.sql_statement_10(Feature_Table_Builder_v0, inpatient_visits)

    Feature_Table_Builder = a.sql_statement_02(Feature_Table_Builder_v0, tot_icu_days_calc, tot_ip_days_calc)

    # 2_med_feature_table




if __name__ == "__main__":
    main()