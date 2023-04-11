from src.STEP1_feature import a_cohort as a
from src.STEP1_feature import d_lab_measures as d
from src.STEP1_feature import e_comorbidity as e
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

    # 4_lab_measure_table
    measure_person = d.measurement_person(measurement, Feature_Table_Builder, concept)
    pre_pre_measurement = d.sql_statement_05(Feature_Table_Builder, measure_person )

    pre_measurement = d.sql_statement_04( Feature_Table_Builder, measure_person )
    covid_measurement = d.sql_statement_00(Feature_Table_Builder, measure_person )
    post_measurement = d.sql_statement_03(Feature_Table_Builder, measure_person )

    four_windows_measure = d.sql_statement_01(covid_measurement, post_measurement, pre_measurement, pre_pre_measurement)

    lab_measures_clean = d.sql_statement_02(four_windows_measure)

    # 5_comorbidity_table
    high_level_condition_occur = e.sql_statement_01(Feature_Table_Builder, condition_occurrence, concept)
    comorbidity_counts = e.sql_statement_00()

    # 6_covid_measures
    covid_person = spark.sql(sql_statement_02())

    covid_measure_indicators = spark.sql(sql_statement_00())
    covid_window = spark.sql(sql_statement_03())
    post_covid = spark.sql(sql_statement_05())

    pos_neg_date = spark.sql(sql_statement_04())  # wrong input file location?

    start_end_date_df = start_end_date(pos_neg_date)  # missing?

    covid_measures = spark.sql(sql_statement_01())




if __name__ == "__main__":
    main()