from src.STEP1_feature import a_cohort as a, b_medication as b, c_diagnosis as c, \
    d_lab_measures as d, e_comorbidity as e, f_covid_measures as f, h_observation as h

import os
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
    DrugConcepts = b.sql_statement_00(concept)
    Drugs_for_These_Patients = b.sql_statement_01(Feature_Table_Builder, drug_exposure)

    drugRollUp = b.sql_statement_04(DrugConcepts, Drugs_for_These_Patients)

    covid_drugs = b.sql_statement_02(Feature_Table_Builder, drugRollUp)
    pre_pre_drugs = b.sql_statement_10(Feature_Table_Builder, drugRollUp)
    pre_drugs = b.sql_statement_07(Feature_Table_Builder, drugRollUp)
    post_drugs = b.sql_statement_05(Feature_Table_Builder, drugRollUp)

    covidtbl = b.sql_statement_03(Feature_Table_Builder, covid_drugs, microvisits_to_macrovisits)
    prepretbl = b.sql_statement_11(Feature_Table_Builder, pre_pre_drugs microvisits_to_macrovisits)
    pretbl = b.sql_statement_12(Feature_Table_Builder, pre_drugs, microvisits_to_macrovisits)
    posttbl = b.sql_statement_06(Feature_Table_Builder, post_drugs, microvisits_to_macrovisits)

    pre_post_med_count = b.sql_statement_08(covidtbl, posttbl, prepretbl, pretbl)

    pre_post_med_count_clean = b.sql_statement_09(Feature_Table_Builder, pre_post_med_count)
    
    # 3_dx_feature_table
    pre_pre_condition = c.sql_statement_05(Feature_Table_Builder, condition_occurrence)
    pre_condition = c.sql_statement_03(Feature_Table_Builder, condition_occurrence)
    covid_condition = c.sql_statement_00(Feature_Table_Builder, condition_occurrence)
    post_condition = c.sql_statement_02(Feature_Table_Builder, condition_occurrence)

    four_windows_dx_counts = c.sql_statement_01(Feature_Table_Builder, microvisits_to_macrovisits, pre_pre_condition, pre_condition, covid_condition, post_condition)

    pre_post_dx_count_clean = c.sql_statement_04(Feature_Table_Builder, four_windows_dx_counts)

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
    comorbidity_counts = e.sql_statement_00(Feature_Table_Builder, high_level_condition_occur)

    # 6_covid_measures
    covid_person = f.sql_statement_02(Feature_table_builder, measurement, concept)

    covid_measure_indicators = f.sql_statement_00(covid_person)
    covid_window = f.sql_statement_03(covid_person)
    post_covid = f.sql_statement_05(covid_person)

    pos_neg_date = f.spark.sql(sql_statement_04())  # wrong input file location?

    start_end_date_df = f.start_end_date(pos_neg_date)  # missing?

    covid_measures = f.sql_statement_01(covid_measure_indicators, start_end_date)

    # 9_obs_person: empty observation file
    obs_person = h.sql_statement_00(Feature_Table_Builder, observation, concept)
    obs_person_clean = h.sql_statement_01(obs_person)
    obs_person_pivot_df = h.obs_person_pivot(obs_person_clean)




if __name__ == "__main__":
    main()