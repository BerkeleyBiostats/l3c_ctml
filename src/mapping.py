
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