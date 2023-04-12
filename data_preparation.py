from src.STEP1_feature import a_cohort as a, b_medication as b, c_diagnosis as c, \
    d_lab_measures as d, e_comorbidity as e, f_covid_measures as f, h_observation as h, i_combine_synthetic_data as i

import os
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType


def read_files(spark, type="training"):
    if type == "training":
        print("Reading training files")
        data_path = "synthetic_data/training"
    else:
        print("Reading testing files")
        data_path = "synthetic_data/testing"

    for path, subdirs, files in os.walk(data_path):
        for name in files:
            if "csv" in name and name[0] != ".":
                # table_fields = [(table_name, f.name, str(f.dataType)) for f in datafile.schema.fields]
                full_file = (os.path.join(path, name))
                df = spark.read.csv(os.path.join(path, name), header=True, inferSchema=True)
                var_name = name[:-4]
                print(full_file)
                globals()[var_name] = df


def preprocess(run_type, spark, long_covid_silver_standard, person, condition_occurrence,
               microvisits_to_macrovisits, concept, drug_exposure, measurement, observation, features):
    # 1_cohort_and_features
    if run_type == "training":
        covid_pasc_index_dates = a.sql_statement_01(long_covid_silver_standard)
    else:
        covid_pasc_index_dates = long_covid_silver_standard
    cohort = a.sql_statement_00(covid_pasc_index_dates, person)
    if run_type == "training":
        long_covid_patients = a.sql_statement_08(covid_pasc_index_dates)
    else:
        long_covid_patients = covid_pasc_index_dates

    hosp_cases = a.sql_statement_04(cohort, condition_occurrence, microvisits_to_macrovisits)

    hosp_and_non = a.sql_statement_06(cohort, hosp_cases)

    Feature_Table_Builder_v0 = a.sql_statement_03(covid_pasc_index_dates, hosp_and_non, microvisits_to_macrovisits)
    icu_visits = a.sql_statement_05(microvisits_to_macrovisits, concept)  # empty for some reason
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
    prepretbl = b.sql_statement_11(Feature_Table_Builder, pre_pre_drugs, microvisits_to_macrovisits)
    pretbl = b.sql_statement_12(Feature_Table_Builder, pre_drugs, microvisits_to_macrovisits)
    posttbl = b.sql_statement_06(Feature_Table_Builder, post_drugs, microvisits_to_macrovisits)

    pre_post_med_count = b.sql_statement_08(covidtbl, posttbl, prepretbl, pretbl)

    pre_post_med_count_clean = b.sql_statement_09(Feature_Table_Builder, pre_post_med_count)

    # 3_dx_feature_table
    pre_pre_condition = c.sql_statement_05(Feature_Table_Builder, condition_occurrence)
    pre_condition = c.sql_statement_03(Feature_Table_Builder, condition_occurrence)
    covid_condition = c.sql_statement_00(Feature_Table_Builder, condition_occurrence)
    post_condition = c.sql_statement_02(Feature_Table_Builder, condition_occurrence)

    four_windows_dx_counts = c.sql_statement_01(Feature_Table_Builder, microvisits_to_macrovisits, pre_pre_condition,
                                                pre_condition, covid_condition, post_condition)

    pre_post_dx_count_clean = c.sql_statement_04(Feature_Table_Builder, four_windows_dx_counts)

    # 4_lab_measure_table
    measure_person = d.measurement_person(measurement, Feature_Table_Builder, concept)
    pre_pre_measurement = d.sql_statement_05(Feature_Table_Builder, measure_person)

    pre_measurement = d.sql_statement_04(Feature_Table_Builder, measure_person)
    covid_measurement = d.sql_statement_00(Feature_Table_Builder, measure_person)
    post_measurement = d.sql_statement_03(Feature_Table_Builder, measure_person)

    four_windows_measure = d.sql_statement_01(covid_measurement, post_measurement, pre_measurement, pre_pre_measurement)

    lab_measures_clean = d.sql_statement_02(four_windows_measure)

    # 5_comorbidity_table
    high_level_condition_occur = e.sql_statement_01(Feature_Table_Builder, condition_occurrence, concept)
    comorbidity_counts = e.sql_statement_00(Feature_Table_Builder, high_level_condition_occur)

    # 6_covid_measures: empty
    covid_person = f.sql_statement_02(Feature_Table_Builder, measurement, concept)

    covid_measure_indicators = f.sql_statement_00(covid_person)
    covid_window = f.sql_statement_03(covid_person)
    post_covid = f.sql_statement_05(covid_person)

    pos_neg_date = f.sql_statement_04(covid_window, post_covid)  # wrong input file location?

    start_end_date_df = f.start_end_date(pos_neg_date)

    covid_measures = f.sql_statement_01(covid_measure_indicators, start_end_date_df)

    # 9_obs_person: empty observation file
    obs_person = h.sql_statement_00(Feature_Table_Builder, observation, concept)
    obs_person_clean = h.sql_statement_01(obs_person)
    obs_person_pivot_df = h.obs_person_pivot(obs_person_clean)

    # 10: combine data
    pre_post_med_final_distinct_df = i.pre_post_med_final_distinct(pre_post_med_count_clean)
    add_labels_df = i.add_labels(run_type, spark, pre_post_dx_count_clean, pre_post_med_count_clean, long_covid_patients,
                                 Feature_Table_Builder)

    condition_rollup_df = i.condition_rollup(long_covid_patients, pre_post_dx_count_clean, concept)

    parent_condition_rollup = i.parent_condition_rollup(condition_rollup_df)

    final_rollups_df = i.final_rollups(condition_rollup_df, parent_condition_rollup)
    pre_post_more_in_dx_calc_df = i.pre_post_more_in_dx_calc(pre_post_dx_count_clean)

    add_alt_rollup_df = i.add_alt_rollup(final_rollups_df, pre_post_more_in_dx_calc_df)

    pre_post_dx_final_df = i.pre_post_dx_final(add_alt_rollup_df)
    count_dx_pre_and_post_df = i.count_dx_pre_and_post(pre_post_dx_final_df)
    final_df = i.feature_table_all_patients_dx_drug(features, add_labels_df, pre_post_dx_final_df,
                                                    count_dx_pre_and_post_df,
                                                    pre_post_med_final_distinct_df, comorbidity_counts,
                                                    lab_measures_clean,
                                                    covid_measures, obs_person_pivot_df)
    return final_df


def main():
    spark = SparkSession.builder \
        .master("local[1]") \
        .appName("l3c_ctml") \
        .getOrCreate()

    # Read CSV file into table

    read_files(spark, type="training")
    print("Start preprocessing")
    training_df = preprocess(spark, long_covid_silver_standard, person, condition_occurrence,
               microvisits_to_macrovisits, concept, drug_exposure, measurement, observation, features)
    print("Finish preprocessing")
    training_df.write.csv('training.csv')
    print("Training saved")


    read_files(spark, type="training")
    testing_df = preprocess(spark, long_covid_silver_standard, person, condition_occurrence,
               microvisits_to_macrovisits, concept, drug_exposure, measurement, observation, features)
    testing_df.write.csv('testing.csv')



if __name__ == "__main__":
    main()
