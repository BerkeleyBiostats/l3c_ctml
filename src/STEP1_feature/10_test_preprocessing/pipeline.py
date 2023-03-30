from pyspark.sql import functions as F
import re
from pyspark.sql.types import IntegerType
from pyspark.sql.functions import udf

# Read in the file containing the list of model features, one per line
# returns cols_for_model, which is used in several other functions
def read_model_columns():

    f = open('feature_list.txt', 'r')
    lines = f.readlines()
    cols_for_model = [l.strip() for l in lines]
    f.close()
    return cols_for_model

def pivot_covid(df):
    # make the column name standard
    df = df.withColumn("measure_type", F.lower(F.regexp_replace(df["measure_type"], "[^A-Za-z_0-9]", "_" )))
    df = df.groupby("person_id").pivot("measure_type").agg(
        F.max("c_any_measure").alias("measure_covid_ind"),
        F.max("c_any_pos").alias("positive_covid_ind"),
        F.max("c_covid_length").alias("covid_length_covid"),
        F.max("c_impute_covid_length").alias("impute_covid_ind"),
        F.max("post_any_measure").alias("measure_post_ind"),
        F.max("post_any_pos").alias("positive_post_ind"),
        F.max("post_covid_length").alias("covid_length_post"),
        F.max("post_impute_covid_length").alias("impute_post_ind"))
    df = df.fillna(0)
    return df

def pivot_dx(dx_df, cols_for_model):

    # Filter only to dx used in model and then pivot
    # This greatly improves performance as both spark and pandas do poorly with very wide datasets

    dx_df = dx_df.filter(dx_df["high_level_condition"].isin(cols_for_model))    
    dx_df = dx_df.groupby("person_id").pivot("high_level_condition").agg(
        F.max("pre_dx_count_sum").alias("pre_dx"),
        F.max("pre_pre_dx_count_sum").alias("pp_dx"),
        F.max("covid_dx_count_sum").alias("c_dx"),
        F.max("post_dx_count_sum").alias("post_dx"))
    
    # the absence of a diagnosis record means it is neither greater in post or only in post
    dx_df = dx_df.fillna(0)

    return dx_df

def pivot_meds(med_df, cols_for_model):
    
    # Filter only to meds used in the canonical all patients model and then pivot
    # This greatly improves performance as both spark and pandas do poorly with very wide datasets
    
    med_df = med_df.filter(med_df["ingredient"].isin(cols_for_model))    
    # med_df = med_df.groupby("person_id").pivot("ingredient").agg(F.max("post_only_med").alias("post_only_med"))
    med_df = med_df.groupby("person_id").pivot("ingredient").agg(
        F.max("pre_med_count").alias("pre_med"),
        F.max("pre_pre_med_count").alias("pp_med"),
        F.max("covid_med_count").alias("covid_med"),
        F.max("post_med_count").alias("post_med"))
    
    
    # if there is no row for a patient:drug combination, there will be nulls in the pivot.  This is converted to 0 to represent the absence of a drug exposure.
    med_df = med_df.fillna(0)

    return med_df

def pivot_measure(measure_df):
    
    # Filter only to measures used in the canonical all patients model and then pivot
    # This greatly improves performance as both spark and pandas do poorly with very wide datasets
    measure_df = measure_df.drop('measurement_concept_id')
    measure_df = measure_df.withColumn("measurement_concept_name", F.lower(F.regexp_replace(measure_df["measurement_concept_name"], "[^A-Za-z_0-9]", "_" )))
    
    measure_df = measure_df.groupby("person_id").pivot("measurement_concept_name").agg(
        F.max("pre_pre_max").alias("pp_max"),
        F.max("pre_pre_min").alias("pp_min"),
        F.max("pre_pre_avg").alias("pp_avg"),
        F.max("pre_max").alias("pre_max"),
        F.max("pre_min").alias("pre_pre_min"),
        F.max("pre_avg").alias("pre_avg"),
        F.max("covid_max").alias("covid_max"),
        F.max("covid_min").alias("covid_min"),
        F.max("covid_avg").alias("covid_avg"),

        F.max("post_max").alias("post_max"),
        F.max("post_min").alias("post_min"),
        F.max("post_avg").alias("post_avg"))
    
    # if there is no row for a patient:drug combination, there will be nulls in the pivot.  This is converted to 0 to represent the absence of a measurement.
    # measure_df = measure_df.fillna('NA')

    return measure_df

def pivot_nlp(nlp_df):
    nlp_df = nlp_df.withColumn("note_nlp_concept_name", F.lower(F.regexp_replace(nlp_df["note_nlp_concept_name"], "[^A-Za-z_0-9]", "_" )))
    nlp_df = nlp_df.groupby("person_id").pivot("note_nlp_concept_name").agg(
        F.max("pre_nlp_count").alias("pre_nlp"),
        F.max("pre_pre_nlp_count").alias("pp_nlp"),
        F.max("covid_nlp_count").alias("covid_nlp"),
        F.max("post_nlp_count").alias("post_nlp"))
        
    nlp_df = nlp_df.fillna(0)

    return nlp_df

def pivot_device(device_df):
    device_df = device_df.withColumn("device_concept_name", F.lower(F.regexp_replace(device_df["device_concept_name"], "[^A-Za-z_0-9]", "_" )))
    device_df = device_df.groupby("person_id").pivot("device_concept_name").agg(
        F.max("pre_device_count").alias("pre_device"),
        F.max("pre_pre_device_count").alias("pp_device"),
        F.max("covid_device_count").alias("covid_device"),
        F.max("post_device_count").alias("post_device"))
        
    device_df = device_df.fillna(0)

    return device_df

def build_final_feature_table(med_df, dx_df, add_labels, count_dx_pre_and_post, measure_df, covid_df, device_df):

    count_dx = count_dx_pre_and_post

    df = add_labels.join(med_df, on="person_id",  how="left")
    df = df.join(dx_df, on='person_id', how='left')
    df = df.join(count_dx, on='person_id', how='left')
    # Some patients in the condition data aren't in the drug dataset
    # meaning they don't have any drugs in the relevant period 
    df = df.fillna(0)

    df = df.join(measure_df, on='person_id', how='left')
    df = df.fillna(-999)

    convert_ind =  udf(lambda old_c: 1 if old_c ==-999 else 0, IntegerType())

    # create new indicator columns and join them with the df
    ind_df = df.select([convert_ind(df[col_name]).alias(col_name+'_ind') if col_name != 'person_id' else df[col_name] for col_name in measure_df.columns])
    ind_df = ind_df.withColumnRenamed('person_id_ind', 'person_id')
    df = df.join(ind_df, on='person_id', how='left')

    df = df.na.replace(-999, 0)

    # left join with covid measures

    df = df.join(covid_df, on='person_id', how='left')
    # df = df.join(nlp_df, on='person_id', how='left')
    df = df.join(device_df, on='person_id', how='left')
    df = df.fillna(0)
    result = df
    
    drop_cols = []
    cols = result.columns
    for c in cols:

        # drop ALL the race and ethnicity columns
        # if re.match('^race_', c) or re.match('^ethn', c):
            # drop_cols.append(c)

        # Among the sex columns, keep only male and unknown
        if re.match('^sex_', c) and c != 'sex_male' and c != 'sex_unknown':
            drop_cols.append(c)

        # Among the ethn columns, keep only hispanic_or_latino and unknown
        if re.match('^ethn_', c) and c != 'ethn_hispanic_or_latino' and c != 'ethn_unknown':
            drop_cols.append(c)

    # # drop the 'no' versions of disease history, keeping the 'yes' versions
    # # drop disorder by body site - too vague
    drop_cols.extend(["diabetes_ind_no", "kidney_ind_no", "chf_ind_no", "chronicpulm_ind_no", "patient_group", "disorder_by_body_site"])

    result = result.drop(*drop_cols)
    return result

@transform_pandas(
    Output(rid="ri.vector.main.execute.99b5685a-2193-4bf1-be8e-e8fd2eefcd39"),
    final_rollups=Input(rid="ri.vector.main.execute.53c19bcb-beaa-4427-bf2e-5c3739088003"),
    pre_post_dx_count_clean=Input(rid="ri.vector.main.execute.73433ca0-fec4-4210-a0b9-2946683617bc")
)
# STEP 7: add_alt_rollup ######################################################################################

from pyspark.sql import functions as F

# Output pyspark dataframe:  add_alt_rollup
# Input  pyspark dataframe:  pre_post_dx_more_in_post, final_rollups

def add_alt_rollup(final_rollups, pre_post_dx_count_clean):

    pre_post_dx_final = pre_post_dx_count_clean

    condition = [pre_post_dx_final['condition_concept_name'] == final_rollups['child_concept_name'] ]
    
    df = pre_post_dx_final.join(final_rollups.select(['child_concept_name', 'parent_concept_name']), how='left', on=condition)
    
    df = df.drop('child_concept_name')
    df = df.withColumnRenamed('parent_concept_name', 'high_level_condition')

    return df

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.94b738b9-5d98-4b1b-bcff-cad647b2456d"),
    Feature_table_builder=Input(rid="ri.foundry.main.dataset.e0264d38-57aa-4509-9245-3d008b0526c4"),
    Long_COVID_Silver_Standard_Blinded=Input(rid="ri.foundry.main.dataset.cb65632b-bdff-4aa9-8696-91bc6667e2ba"),
    pre_post_dx_count_clean=Input(rid="ri.vector.main.execute.73433ca0-fec4-4210-a0b9-2946683617bc"),
    pre_post_med_count_clean=Input(rid="ri.foundry.main.dataset.c729b93f-caf1-4a7d-ac0d-569188c4526e")
)
# STEP 2: add_labels ##################################################################################### 
# union the med and diagnosis, encode categorical variables, add labels
from pyspark.sql import functions as F
import pandas as pd
from pyspark.context import SparkContext
ctx = SparkContext.getOrCreate()

# Output pyspark dataframe:  add_labels
# Input  pyspark dataframe:  SparkContext, pre_post_dx_count_clean, pre_post_med_count_clean, long_covid_patients

# https://spark.apache.org/docs/3.1.1/api/python/reference/api/pyspark.SparkContext.html
def add_labels(pre_post_dx_count_clean, pre_post_med_count_clean, Long_COVID_Silver_Standard_Blinded, Feature_table_builder):

    # total patients

    # There are patients in the meds table that are not in the dx table and vice versa
    # This transform generates the full list of patients in either table (the union)
    # Along with the fields that are defined for every patient (sex, group, age etc)
    target_columns = ['person_id', 'sex', 'patient_group', 'apprx_age', 'race', 'ethn', 'tot_long_data_days', 
    'op_post_visit_ratio', 'post_ip_visit_ratio', "covid_ip_visit_ratio", "post_icu_visit_ratio", "covid_icu_visit_ratio", 'min_covid_dt', 'site_id']

    # df_unique_med_rows = pre_post_med_count_clean.select(target_columns).distinct()
    
    # df_unique_dx_rows = pre_post_dx_count_clean.select(target_columns).distinct()

    df_unique_cohort_rows = Feature_table_builder.select(target_columns).distinct()

    # pyspark union is equivalent to union all in SQL
    # df = df_unique_med_rows.union(df_unique_dx_rows).distinct()
    # df = df.union(df_unique_cohort_rows).distinct()
    df =  df_unique_cohort_rows
    
    # Encode dummy variables
    def cleanup(s):
        if s.lower() in ('unknown', 'gender unknown', 'no matching concept'):
            return 'unknown'
        else:
            return s

    df = df.toPandas() # convert spark to pandas

    df['sex'] = [cleanup(s) for s in df['sex']]
    # also need to clean race and ethn?

    df = df.replace({'race':{'African American' : 'Asian or Pacific islander', 
                             'Asian Indian' : 'Asian or Pacific islander',
                             'Chinese' : 'Asian or Pacific islander',
                             'Asian' : 'Asian or Pacific islander',
                             'Native Hawaiian or Other Pacific Islander' : 'Asian or Pacific islander',
                             'Other Pacific Islander' : 'Asian or Pacific islander',
                             'Black' : 'Black or African American',
                             'Hispanic' : 'Other',
                             # Federal policy defines “Hispanic” not as a race, but as an ethnicity. And it prescribes that Hispanics can in fact be of any race.
                             'More than one race' : 'Other',
                             'Multiple race' : 'Other',
                             'Multiple races' : 'Other',
                             'Other Race' : 'Other',
                             'No information' : 'Unknown',
                             'No matching concept' : 'Unknown',
                             'Refuse to answer' : 'Unknown',
                             'Unknown racial group' : 'Unknown'
                              }})

    df = df.replace({'ethn':{'No information' : 'Unknown',
                             'No matching concept' : 'Unknown',
                             'Other' : 'Unknown',
                             'Other/Unknown' : 'Unknown'
                              }})
    
    # encode these caterogical variables as binaries
    df = pd.get_dummies(df, columns=["sex","race","ethn", "site_id"])
    df = df.rename(columns = lambda c: str.lower(c.replace(" ", "_")))
    df = spark.createDataFrame(df) # convert pandas back to spark

    
    # Add Labels
    final_cols = df.columns
    #final_cols.extend(["long_covid", "hospitalized", 'date_encode', 'season_covid'])
    final_cols.extend(["hospitalized", 'date_encode', 'season_covid'])
    final_cols.remove('min_covid_dt')

    df = df.join(Long_COVID_Silver_Standard_Blinded, on='person_id', how='inner')
    
    # Join with the long covid clinic data to build our labels (long_covid)
    # df = df.withColumn("long_covid", F.when(df["pasc_index"].isNotNull(), 1).otherwise(0))
    df = df.withColumn("hospitalized", F.when(df["patient_group"] == 'CASE_HOSP', 1).otherwise(0))

    # add month, year based on covid_index
    # earliest_dt = min(df['min_covid_dt'])
    earliest_dt = df.agg({"min_covid_dt": "min"}).collect()[0][0]
    df = df.withColumn('date_encode', F.months_between(df['min_covid_dt'], F.lit(earliest_dt), False).cast(IntegerType()))
    df = df.withColumn('month', F.month(df['min_covid_dt']))
    df = df.withColumn('season_covid', F.when((df['month']>=3) & (df['month']<=5), 1).\
                                       when((df['month']>=6) & (df['month']<=8), 2).\
                                       when((df['month']>=9) & (df['month']<=11),3).\
                                       otherwise(4))

    df = df.select(final_cols)

    return df

@transform_pandas(
    Output(rid="ri.vector.main.execute.5424f030-d12d-482a-9a50-0dea98c1e124"),
    Long_COVID_Silver_Standard_Blinded=Input(rid="ri.foundry.main.dataset.cb65632b-bdff-4aa9-8696-91bc6667e2ba"),
    concept=Input(rid="ri.foundry.main.dataset.5cb3c4a3-327a-47bf-a8bf-daf0cafe6772"),
    concept_ancestor=Input(rid="ri.foundry.main.dataset.c5e0521a-147e-4608-b71e-8f53bcdbe03c"),
    pre_post_dx_count_clean=Input(rid="ri.vector.main.execute.73433ca0-fec4-4210-a0b9-2946683617bc")
)
# STEP 4:  condition_rollup #####################################################################################

# Output pyspark dataframe:  condition_rollup
# Input  pyspark dataframe:  long_covid_patients, pre_post_dx_count_clean, concept_ancestor, concept
# NOTE: concept and concept_ancestor are standard OMOP vocabulary tables

def condition_rollup(Long_COVID_Silver_Standard_Blinded, pre_post_dx_count_clean, concept_ancestor, concept):
   
    pp = pre_post_dx_count_clean.alias('pp')
    ca = concept_ancestor.alias('ca')
    ct = concept.alias('ct')
    lc = Long_COVID_Silver_Standard_Blinded.alias('lc')

    df = pp.join(lc, on='person_id', how='inner')
    df = df.join(ca, on=[df.condition_concept_id == ca.descendant_concept_id], how='inner')
    df = df.join(ct, on=[df.ancestor_concept_id  == ct.concept_id], how='inner')

    df = df.filter( ~df['concept_name'].isin(
                                                ['General problem AND/OR complaint',
                                                'Disease',
                                                'Sequelae of disorders classified by disorder-system',
                                                'Sequela of disorder',
                                                'Sequela',
                                                'Recurrent disease',
                                                'Problem',
                                                'Acute disease',
                                                'Chronic disease',
                                                'Complication'
                                                ]))
    
    generic_codes = ['finding', 'disorder of', 'by site', 'right', 'left']

    for gc in generic_codes:
        df = df.filter( ~F.lower(ct.concept_name).like('%' + gc + '%') )
        
        if gc not in ['right', 'left']:
            df = df.filter( ~F.lower(pp.condition_concept_name).like('%' + gc + '%') )

    df = df.filter(ca.min_levels_of_separation.between(0,2))

    
    df = df.groupby(['ct.concept_name', 'pp.condition_concept_name', 'pp.condition_concept_id', 
                    'ca.min_levels_of_separation', 'ca.max_levels_of_separation']).agg(F.countDistinct('pp.person_id').alias('ptct_training'))

    df = df.withColumnRenamed('concept_name', 'parent_concept_name')
    df = df.withColumnRenamed('condition_concept_name', 'child_concept_name')
    df = df.withColumnRenamed('min_levels_of_separation', 'min_hops_bt_parent_child')
    df = df.withColumnRenamed('max_levels_of_separation', 'max_hops_bt_parent_child')
    df = df.withColumnRenamed('condition_concept_id', 'child_concept_id')

    return df

    

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.db50925d-bf61-4ca3-8bb8-a2fdd705e0af"),
    pre_post_dx_final=Input(rid="ri.foundry.main.dataset.bd5cdf89-b394-401b-98f8-60371eaa2948")
)
# STEP 9: count_dx_pre_and_post ######################################################################################
# calculate total diagnosis counts in 4 windows by person

from pyspark.sql import functions as F

# Output pyspark dataframe:  count_dx_pre_and_post
# Input  pyspark dataframe:  pre_post_dx_final

def count_dx_pre_and_post(pre_post_dx_final):

    
    # sum the total pre and post dx (of any kind, for use later in a filter)

    df = pre_post_dx_final

    total_pre_pre = df.groupby('person_id').agg(F.sum('pre_pre_dx_count_sum').alias('total_pre_pre_dx'))

    total_pre = df.groupby('person_id').agg(F.sum('pre_dx_count_sum').alias('total_pre_dx'))

    total_covid = df.groupby('person_id').agg(F.sum('covid_dx_count_sum').alias('total_covid_dx'))
    
    total_post = df.groupby('person_id').agg(F.sum('post_dx_count_sum').alias('total_post_dx'))

    distinct_people = df.select('person_id').distinct()

    temp_left_join = lambda all_dat, dat: all_dat.join(dat, on="person_id", how="left") # temp fn for left joining 4 tables
    result = distinct_people
    for dat in [total_pre_pre, total_pre, total_covid, total_post]:
        result = temp_left_join(result, dat) 

    # result = distinct_people.join(total_pre_pre, on='person_id', how='left')
    # result = result.join(total_pre, on='person_id', how='left')
    # result = result.join(total_post, on='person_id', how='left')

    return result
    

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.da151535-e48f-4c94-a8ca-3638125d5c13"),
    add_labels=Input(rid="ri.foundry.main.dataset.94b738b9-5d98-4b1b-bcff-cad647b2456d"),
    comorbidity_counts=Input(rid="ri.vector.main.execute.1b318b0b-fa44-4315-90c7-509b4bedc06b"),
    count_dx_pre_and_post=Input(rid="ri.foundry.main.dataset.db50925d-bf61-4ca3-8bb8-a2fdd705e0af"),
    covid_measures=Input(rid="ri.foundry.main.dataset.00f71213-03c7-44f8-be40-520411e33272"),
    device_count_clean=Input(rid="ri.foundry.main.dataset.bfb42402-89ee-4333-9677-40d88ea630dd"),
    features=Input(rid="ri.foundry.main.dataset.fae33c93-0560-420c-9766-fb68e86eb88b"),
    lab_measures_clean=Input(rid="ri.foundry.main.dataset.26ad74be-fcb8-4a41-b373-501854feb6c3"),
    obs_person_pivot=Input(rid="ri.foundry.main.dataset.61c2f4cd-04e7-4e27-948c-035771aba47b"),
    pre_post_dx_final=Input(rid="ri.foundry.main.dataset.bd5cdf89-b394-401b-98f8-60371eaa2948"),
    pre_post_med_final_distinct=Input(rid="ri.foundry.main.dataset.070f2bc2-af22-4a7d-ab18-f49037d80aed"),
    severity_table=Input(rid="ri.foundry.main.dataset.955b03ad-9aff-46db-9ad5-41d0c4f4fdb9")
)
 # STEP 10: feature_table_all_patients ######################################################################################

# Output pyspark dataframe:  feature_table_all_patients
# Input  pyspark dataframe:  training_data, add_labels, pre_post_med_final, pre_post_dx_final, count_dx_pre_and_post

def feature_table_all_patients_dx_drug(features, add_labels, pre_post_dx_final, count_dx_pre_and_post, pre_post_med_final_distinct, comorbidity_counts, lab_measures_clean, covid_measures, device_count_clean, obs_person_pivot, severity_table):

    # Filter only to meds used in the canonical all patients model and then pivot
    features_p = features.toPandas()
    f=features_p['features'].tolist()
    med_df = pivot_meds(pre_post_med_final_distinct, f)
    
    dx_df = pivot_dx(pre_post_dx_final, f)
    lab_df = pivot_measure(lab_measures_clean)
    covid_df = pivot_covid(covid_measures)

    # nlp_df = pivot_nlp(Nlp_count_clean)
    device_df = pivot_device(device_count_clean)

    result = build_final_feature_table(med_df, dx_df, add_labels, count_dx_pre_and_post, lab_df, covid_df, device_df)
    result = result.join(comorbidity_counts, on='person_id', how='left')
    result = result.join(obs_person_pivot, on='person_id', how='left')
    result = result.join(severity_table, on = 'person_id', how='left')
    result = result.fillna(0)
    # print(result.count())

    return result
    

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.a000ff39-5346-446b-bae0-774be581f4e2"),
    Feature_table_all_patients_dx_drug_train=Input(rid="ri.foundry.main.dataset.280e0507-3677-4071-9953-58e698b78c92"),
    feature_table_all_patients_dx_drug=Input(rid="ri.foundry.main.dataset.da151535-e48f-4c94-a8ca-3638125d5c13")
)
from pyspark.sql import functions as F
import pandas as pd
from pyspark.context import SparkContext
ctx = SparkContext.getOrCreate()

def feature_table_final_testing_set(Feature_table_all_patients_dx_drug_train, feature_table_all_patients_dx_drug) :
    train_cols = Feature_table_all_patients_dx_drug_train.toPandas().columns.tolist()

    df = feature_table_all_patients_dx_drug.toPandas()
    df = df.reindex(columns=[*train_cols], fill_value=0)
    df = df.drop('long_covid', axis=1)
    df = spark.createDataFrame(df)  # convert pandas back to spark
    return df

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.fae33c93-0560-420c-9766-fb68e86eb88b")
)
from pyspark.sql.types import *
def features():
    schema = StructType([StructField("features", StringType(), True)])
    return spark.createDataFrame([["difficulty_breathing"],["apprx_age"],["fatigue"],["op_post_visit_ratio"],["dyspnea"],["ip_post_visit_ratio"],["albuterol"],["hospitalized"],["sex_male"],["fluticasone"],["palpitations"],["mental_disorder"],["uncomplicated_asthma"],["chronic_pain"],["malaise"],["chronic_fatigue_syndrome"],["formoterol"],["tachycardia"],["metabolic_disease"],["chest_pain"],["inflammation_of_specific_body_organs"],["impaired_cognition"],["diarrhea"],["acetaminophen"],["dyssomnia"],["anxiety_disorder"],["cough"],["anxiety"],["muscle_pain"],["interstitial_lung_disease"],["migraine"],["degenerative_disorder"],["viral_lower_respiratory_infection"],["promethazine"],["deficiency_of_micronutrients"],["asthma"],["disorder_characterized_by_pain"],["apixaban"],["lesion_of_lung"],["inflammation_of_specific_body_systems"],["breathing_related_sleep_disorder"],["chronic_nervous_system_disorder"],["iopamidol"],["loss_of_sense_of_smell"],["amitriptyline"],["sleep_disorder"],["pain_of_truncal_structure"],["neurosis"],["headache"],["tracheobronchial_disorder"],["communication_disorder"],["amnesia"],["hypoxemia"],["lower_respiratory_infection_caused_by_sars_cov_2"],["bleeding"],["amoxicillin"],["disorder_due_to_infection"],["chronic_sinusitis"],["pain_in_lower_limb"],["furosemide"],["buspirone"],["vascular_disorder"],["memory_impairment"],["insomnia"],["budesonide"],["prednisone"],["pneumonia_caused_by_sars_cov_2"],["clavulanate"],["dizziness"],["neuropathy"],["iron_deficiency_anemia_due_to_blood_loss"],["estradiol"],["ceftriaxone"],["shoulder_joint_pain"],["sexually_active"],["abdominal_pain"],["skin_sensation_disturbance"],["ketorolac"],["depressive_disorder"],["hyperlipidemia"],["chronic_kidney_disease_due_to_hypertension"],["spondylosis"],["vascular_headache"],["fibrosis_of_lung"],["acute_respiratory_disease"],["chronic_cough"],["osteoporosis"],["lorazepam"],["connective_tissue_disorder_by_body_site"],["adjustment_disorder"],["benzonatate"],["shoulder_pain"],["mineral_deficiency"],["obesity"],["epinephrine"],["dependence_on_enabling_machine_or_device"],["dependence_on_respiratory_device"],["inflammation_of_specific_body_structures_or_tissue"],["spironolactone"],["cholecalciferol"],["heart_disease"],["pain"],["major_depression__single_episode"],["meloxicam"],["hydrocortisone"],["collagen_disease"],["headache_disorder"],["hypoxemic_respiratory_failure"],["morphine"],["cardiac_arrhythmia"],["seborrheic_keratosis"],["gabapentin"],["dulaglutide"],["hypertensive_disorder"],["effusion_of_joint"],["moderate_persistent_asthma"],["morbid_obesity"],["seborrheic_dermatitis"],["rbc_count_low"],["blood_chemistry_abnormal"],["acute_digestive_system_disorder"],["sars_cov_2__covid_19__vaccine__mrna_spike_protein"],["influenza_b_virus_antigen"],["pulmonary_function_studies_abnormal"],["sleep_apnea"],["abnormal_presence_of_protein"],["sodium_chloride"],["atropine"],["aspirin"],["cognitive_communication_disorder"],["metronidazole"],["ethinyl_estradiol"],["gadopentetate_dimeglumine"],["traumatic_and_or_non_traumatic_injury_of_anatomical_site"],["colchicine"],["anomaly_of_eye"],["oxycodone"],["osteoarthritis"],["complication_of_pregnancy__childbirth_and_or_the_puerperium"],["allergic_rhinitis"],["dizziness_and_giddiness"],["genitourinary_tract_hemorrhage"],["duloxetine"],["bipolar_disorder"],["vitamin_disease"],["respiratory_obstruction"],["genuine_stress_incontinence"],["chronic_disease_of_respiratory_systemx"],["traumatic_and_or_non_traumatic_injury"],["drug_related_disorder"],["nortriptyline"],["involuntary_movement"],["knee_pain"],["peripheral_nerve_disease"],["gastroesophageal_reflux_disease_without_esophagitis"],["mupirocin"],["fluconazole"],["pure_hypercholesterolemia"],["kidney_disease"],["injury_of_free_lower_limb"],["glaucoma"],["backache"],["tachyarrhythmia"],["myocarditis"],["nitrofurantoin"],["prediabetes"],["sodium_acetate"],["apnea"],["losartan"],["radiology_result_abnormal"],["pantoprazole"],["hemoglobin_low"],["mixed_hyperlipidemia"],["mass_of_soft_tissue"],["levonorgestrel"],["omeprazole"],["allergic_disposition"],["metformin"],["fentanyl"],["spinal_stenosis_of_lumbar_region"],["cyst"],["soft_tissue_lesion"],["altered_bowel_function"],["skin_lesion"],["triamcinolone"],["pain_in_upper_limb"],["acute_respiratory_infections"],["neck_pain"],["guaifenesin"],["disorders_of_initiating_and_maintaining_sleep"],["loratadine"],["vitamin_b12"],["hypercholesterolemia"],["potassium_chloride"],["arthropathy"],["chronic_kidney_disease_due_to_type_2_diabetes_mellitus"],["disease_of_non_coronary_systemic_artery"],["soft_tissue_injury"],["cytopenia"],["fever"]], schema=schema)

@transform_pandas(
    Output(rid="ri.vector.main.execute.53c19bcb-beaa-4427-bf2e-5c3739088003"),
    condition_rollup=Input(rid="ri.vector.main.execute.5424f030-d12d-482a-9a50-0dea98c1e124"),
    parent_condition_rollup=Input(rid="ri.vector.main.execute.59d55b71-6d6a-4778-8e21-675e5bf645c8")
)
# STEP 6: final_rollup ######################################################################################

# Output pyspark dataframe:  final_rollup
# Input  pyspark dataframe:  parent_conditions, condition_rollup

def final_rollups(condition_rollup, parent_condition_rollup):
    pc = parent_condition_rollup
    dm = condition_rollup

    df = pc.join(dm, on='parent_concept_name', how='inner')
    df = df.select(['parent_concept_name', 'child_concept_name']).distinct()

    return df
    

@transform_pandas(
    Output(rid="ri.vector.main.execute.b58177f1-cd68-4be9-be4b-715f899c2a0b"),
    Feature_table_builder=Input(rid="ri.foundry.main.dataset.e0264d38-57aa-4509-9245-3d008b0526c4"),
    measurement=Input(rid="ri.foundry.main.dataset.b7749e49-cf01-4d0a-a154-2f00eecab21e")
)
def measurement_person(Feature_table_builder, measurement):
    # select measurements between dates
    targets = ['Respiratory rate', 'Oxygen saturation in Arterial blood by Pulse oximetry', 'Oxygen saturation in blood', 'Heart rate', 'Systolic blood pressure',
     'Diastolic blood pressure', 'Body temperature', 'Glucose [mass/volume] in Serum or Plasma', 'Body Mass Index (BMI) [Ratio]', 'Hemoglobin [mass/volume] in blood',
     'Potassium [Moles/volume] in serum or plasma', 'Sodium [Moles/volume] in serum or plasma', 'Calcium [Mass/volume] in serum or plasma',
     'Inspired oxygen concentration', 'Pain intensity rating scale',
     'Creatinine [Mass/volume] in Serum or Plasma',
     'Glomerular filtration rate/1.73 sq M.predicted [Volume Rate/Area] in Serum, Plasma or Blood by Creatinine-based formula (MDRD)',
     'Alanine aminotransferase [Enzymatic activity/volume] in Serum or Plasma',
     'Troponin, quantitative',
     'Creatine kinase [Enzymatic activity/volume] in Serum or Plasma',
     'Fibrin degradation products, D-dimer; quantitative',
     'Fibrinogen [Mass/volume] in Platelet poor plasma by Coagulation assay',
     'Platelets [#/volume] in Blood by Automated count',
     'INR in Platelet poor plasma by Coagulation assay',
     'Antinuclear antibodies (ANA)',
     'Vitamin D; 25 hydroxy, includes fraction(s), if performed',
     '25-hydroxyvitamin D3 [Mass/volume] in Serum or Plasma',
     'Cortisol [Mass/volume] in Serum or Plasma',
     'Gammaglobulin (immunoglobulin); IgA, IgD, IgG, IgM, each',
     'Interleukin 6 [Mass/volume] in Serum or Plasma',
     'Creatinine renal clearance predicted by Cockcroft-Gault formula',
     'Creatinine; blood']

    # biomarkers = biomarkers.select('measurement_concept_id', 'biomarker_name')
    persons = Feature_table_builder.select('person_id')

    # biomarker_targets = list(biomarkers.select('concept_name').toPandas())
    # biomarker_ids = list(biomarkers.select('concept_id').toPandas())
    # covid_targets = []
    

    # df_biomarkers = measurement\
    # .select('person_id','measurement_date','measurement_concept_id', 'harmonized_value_as_number','value_as_concept_id')\
    # .where(F.col('measurement_date').isNotNull()) \
    # .where(F.col('harmonized_value_as_number').isNotNull()) \
    # .withColumnRenamed('measurement_date','visit_date') \
    # .join(biomarkers, 'measurement_concept_id', 'inner') \
    # .withColumnRenamed('biomarker_name', 'measurement_concept_name')\
    # .withColumnRenamed('measurement_date','visit_date') \
    # .join(persons,'person_id','inner')

    # df_biomarkers = df_biomarkers.select('person_id','visit_date','measurement_concept_id', 'harmonized_value_as_number','value_as_concept_id', 'measurement_concept_name')

 
    df = measurement \
    .select('person_id','measurement_date','measurement_concept_id', 'harmonized_value_as_number','value_as_concept_id', 'measurement_concept_name') \
    .where(F.col('measurement_date').isNotNull()) \
    .where(F.col('measurement_concept_name').isin(targets)) \
    .where(F.col('harmonized_value_as_number').isNotNull())\
    .withColumnRenamed('measurement_date','visit_date') \
    .join(persons,'person_id','inner')

    # df = df.union(df_biomarkers)
    return df

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.61c2f4cd-04e7-4e27-948c-035771aba47b"),
    obs_person_clean=Input(rid="ri.vector.main.execute.9baea4ea-ef0d-4496-bacf-c34d0cba82f7")
)
from pyspark.sql import functions as F
def obs_person_pivot(obs_person_clean):
    df = obs_person_clean
    df = df.withColumn("observation_concept_name", F.lower(F.regexp_replace(df["observation_concept_name"], "[^A-Za-z_0-9]", "_" )))
    df = df.groupby("person_id").pivot("observation_concept_name").agg(F.count('person_id').alias('_obs_ind'))
    
    df = df.fillna(0)

    return df
    

@transform_pandas(
    Output(rid="ri.vector.main.execute.59d55b71-6d6a-4778-8e21-675e5bf645c8"),
    condition_rollup=Input(rid="ri.vector.main.execute.5424f030-d12d-482a-9a50-0dea98c1e124")
)
# STEP 5: parent_condition_rollup ######################################################################################

# Output pyspark dataframe:  parent_condition_rollup
# Input  pyspark dataframe:  condition_rollup

from pyspark.sql import functions as F

def parent_condition_rollup(condition_rollup):
    df = condition_rollup
    df = df.groupby('parent_concept_name').agg(F.sum('ptct_training').alias('total_pts'))
    df = df[df['total_pts'] >= 3]
    
    return df

    

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.bd5cdf89-b394-401b-98f8-60371eaa2948"),
    add_alt_rollup=Input(rid="ri.vector.main.execute.99b5685a-2193-4bf1-be8e-e8fd2eefcd39")
)
# STEP 8: pre_post_dx_final ######################################################################################

from pyspark.sql import functions as F

# Output pyspark dataframe:  pre_post_dx_final
# Input  pyspark dataframe:  add_alt_rollup

def pre_post_dx_final(add_alt_rollup):
    
    df = add_alt_rollup
    df = df.filter(df['high_level_condition'] != 'EXCLUDE')
    df = df.filter(df['high_level_condition'].isNotNull())
    df = df.filter(~F.lower(df['high_level_condition']).like('%covid%') )
    df = df.filter(~F.lower(df['high_level_condition']).like('%coronav%') )

    df = df.filter(~F.upper(df['high_level_condition']).like('%post_infectious_disorder%') )
    

    df = df[df["patient_group"].isin('CASE_NONHOSP', 'CASE_HOSP')]
    df = df.withColumn("condition_concept_name", F.lower(F.regexp_replace(df["condition_concept_name"], "[^A-Za-z_0-9]", "_" )))
    df = df.withColumn("high_level_condition", F.lower(F.regexp_replace(df["high_level_condition"], "[^A-Za-z_0-9]", "_" )))

    # Pre-post_dx_final
    # Multiple conditions map to each high_level_condition, so we sum there here
    df = df.groupby(["high_level_condition", "person_id", "patient_group"]).agg(
                            F.sum('pre_pre_dx_count').alias('pre_pre_dx_count_sum'),
                            F.sum('pre_dx_count').alias('pre_dx_count_sum'), 
                            F.sum('covid_dx_count').alias('covid_dx_count_sum'),
                            F.sum('post_dx_count').alias('post_dx_count_sum')
                            )
    df = df.filter(df["high_level_condition"].isNotNull())

    # does the condition occur more often in after the covid acute phase?
    # df = df.withColumn('greater_in_post', F.when(df['post_dx_count_sum'] > df['pre_dx_count_sum'], 1).otherwise(0))

    # # does the condition ONLY occur after the covid acute phase?
    # df = df.withColumn('only_in_post', F.when( ((df['pre_dx_count_sum'] == 0) & (df['post_dx_count_sum'] > 0)), 1).otherwise(0))
    ###
    
    return df

    

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.070f2bc2-af22-4a7d-ab18-f49037d80aed"),
    pre_post_med_count_clean=Input(rid="ri.foundry.main.dataset.c729b93f-caf1-4a7d-ac0d-569188c4526e")
)
# STEP 1: pre_post_med_final_distinct #####################################################################################

from pyspark.sql import functions as F

# Output pyspark dataframe:  pre_post_med_final_distinct
# Input  pyspark dataframe:  pre_post_med_clean

def pre_post_med_final_distinct(pre_post_med_count_clean):
    df = pre_post_med_count_clean
    # df = df.withColumn('post_only_med', F.when(df['pre_med_count'] > 0, 0).otherwise(1)) # indicator whether it is post only

    # Not currently using this feature in the mmodel
    df = df.withColumn('more_in_post',  F.when(df['post_med_count'] > df['pre_med_count'], 1).otherwise(0)) # indicator whether the medication has greater frequency in post-window

    # df = df.select(df['person_id'], df['ancestor_drug_concept_name'].alias('ingredient'), df['post_only_med'])
    df = df.select(df['person_id'], df['ancestor_drug_concept_name'].alias('ingredient'), df['pre_pre_med_count'], df['pre_med_count'], df['covid_med_count'], df['post_med_count'])
    df = df.distinct()

    df = df.withColumn("ingredient", F.lower(F.regexp_replace(df["ingredient"], "[^A-Za-z_0-9]", "_" )))
    
    return df

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.955b03ad-9aff-46db-9ad5-41d0c4f4fdb9"),
    Covid_patient_summary_table=Input(rid="ri.foundry.main.dataset.acd8b822-7179-407d-9828-911c3a7749e0")
)
import pandas as pd
from pyspark.sql import functions as F

def severity_table(Covid_patient_summary_table):
    df = Covid_patient_summary_table
    targets = ['person_id', 'Severity_Type', 'REMDISIVIR_during_covid_hospitalization_indicator', 'COVIDREGIMENCORTICOSTEROIDS_during_covid_hospitalization_indicator', 
    'number_of_COVID_vaccine_doses_before_or_day_of_covid', 'SUBSTANCEABUSE_before_or_day_of_covid_indicator', 'SOLIDORGANORBLOODSTEMCELLTRANSPLANT_before_or_day_of_covid_indicator',
    'TOBACCOSMOKER_before_or_day_of_covid_indicator', 'PULMONARYEMBOLISM_before_or_day_of_covid_indicator','Antibody_Neg_before_or_day_of_covid_indicator',
    'OTHERIMMUNOCOMPROMISED_before_or_day_of_covid_indicator', 'CARDIOMYOPATHIES_before_or_day_of_covid_indicator', 'SYSTEMICCORTICOSTEROIDS_before_or_day_of_covid_indicator', 'PREGNANCY_before_or_day_of_covid_indicator', 'PERIPHERALVASCULARDISEASE_before_or_day_of_covid_indicator', 'HEARTFAILURE_before_or_day_of_covid_indicator', 'RHEUMATOLOGICDISEASE_before_or_day_of_covid_indicator', 'MILDLIVERDISEASE_before_or_day_of_covid_indicator', 'MODERATESEVERELIVERDISEASE_before_or_day_of_covid_indicator', 'state']
    df = df.withColumn('state', F.when((df["state"].isNull()) | (df['state']==0), 'NULL').when(df['state']=='MAINE', 'ME').when(df['state']=='NEW HAMPSHIRE', 'NH').otherwise(df['state']))
    df = df.select(targets).toPandas()
    result = pd.get_dummies(df, columns=["Severity_Type", "state"])
    result = spark.createDataFrame(result)
    return result

    

@transform_pandas(
    Output(rid="ri.vector.main.execute.24643c26-4692-4225-92be-b81a187ae80e"),
    pos_neg_date=Input(rid="ri.vector.main.execute.2cf5493c-fc3e-40da-a018-52f37934e2fe")
)
from pyspark.sql.functions import when, date_add, datediff, expr
def start_end_date(pos_neg_date):
    df = pos_neg_date
    df2 = df.withColumn("end_date", when(df.last_pos_dt.isNotNull() & df.first_neg_dt.isNotNull() & (df.first_neg_dt > df.last_pos_dt),
                                    expr("date_add(last_pos_dt, CAST(datediff(first_neg_dt, last_pos_dt)/2 AS INT))"))
                                    .when(df.last_pos_dt.isNotNull() & (df.first_neg_dt.isNull()), date_add(df.last_pos_dt, 3))
                                    .when(df.last_pos_dt.isNotNull() & df.first_neg_dt.isNotNull() & (df.first_neg_dt < df.last_pos_dt),
                                    date_add(df.last_pos_dt, 3))
                                    .otherwise(None))
    df2 = df2.withColumn('covid_length', datediff(df2.end_date, df2.first_pos_dt))
    df2 = df2.withColumn('impute_covid_length', when(df.last_pos_dt.isNotNull() & (df.first_neg_dt.isNull()), 1)
                                               .otherwise(0))
    return df2

