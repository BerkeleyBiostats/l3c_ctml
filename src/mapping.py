
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