
# 1_cohort_and_features
Hospitalized_Cases = spark.sql(sql_statement_04())
Collect_the_Cohort = spark.sql(sql_statement_00())
Covid_Pasc_Index_Dates = spark.sql(sql_statement_01()) 

long_covid_patients = spark.sql(sql_statement_08()) 

hosp_and_non = spark.sql(sql_statement_06()) 
Feature_Table_Builder_v0 = spark.sql(sql_statement_03()) 
ICU_visits = spark.sql(sql_statement_05()) 
inpatient_visits = spark.sql(sql_statement_07()) 


tot_icu_days_calc = spark.sql(sql_statement_09()) 
tot_ip_days_calc = spark.sql(sql_statement_10()) 

Feature_Table_Builder = spark.sql(sql_statement_02())

# 2_med_feature_table
DrugConcepts = spark.sql(sql_statement_00())
Drugs_for_These_Patients = spark.sql(sql_statement_01())

drugRollUp = spark.sql(sql_statement_04())

covid_drugs = spark.sql(sql_statement_02())
pre_pre_drugs = spark.sql(sql_statement_10())
pre_drugs = spark.sql(sql_statement_07())
post_drugs = spark.sql(sql_statement_05())

covidtbl = spark.sql(sql_statement_03())
prepretbl = spark.sql(sql_statement_11())
pretbl = spark.sql(sql_statement_12())
posttbl = spark.sql(sql_statement_06())

pre_post_med_count = spark.sql(sql_statement_08())

pre_post_med_count_clean = spark.sql(sql_statement_09())

# 3_dx_feature_table
pre_pre_condition = spark.sql(sql_statement_05())
pre_condition = spark.sql(sql_statement_03())
covid_condition = spark.sql(sql_statement_00())
post_condition = spark.sql(sql_statement_02())

four_windows_dx_counts = spark.sql(sql_statement_01())

pre_post_dx_count_clean = spark.sql(sql_statement_04())

# 4_lab_measure_table
measurement_person = spark.sql() #missing?

pre_pre_measurement = spark.sql(sql_statement_05())
pre_measurement = spark.sql(sql_statement_04())
covid_measurement = spark.sql(sql_statement_00())
post_measurement = spark.sql(sql_statement_03())

four_windows_measure = spark.sql(sql_statement_01())

lab_measures_clean = spark.sql(sql_statement_02())

# 5_comorbidity_table
high_level_condition_occur = spark.sql(sql_statement_01())

comorbidity_counts = spark.sql(sql_statement_00())

# 6_covid_measures
covid_person = spark.sql(sql_statement_02())

covid_measure_indicators = spark.sql(sql_statement_00())
covid_window = spark.sql(sql_statement_03())
post_covid = spark.sql(sql_statement_05())

pos_neg_date = spark.sql(sql_statement_04()) # wrong input file location? 

start_end_date = spark.sql() #missing?

covid_measures = spark.sql(sql_statement_01())

average_lengths_covid = spark.sql() #missing?
average_lengths_post = spark.sql() # missing?