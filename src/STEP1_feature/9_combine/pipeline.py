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
    df = df.join(device_df, on='person_id', how='left')
    df = df.fillna(0)
    result = df
    
    drop_cols = []
    cols = result.columns
    for c in cols:

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
    Output(rid="ri.vector.main.execute.2f844f9b-376d-41a0-84f9-9809f3fc0b80"),
    final_rollups=Input(rid="ri.vector.main.execute.4d979157-b9d2-4467-9ff5-652a17ef76a3"),
    pre_post_more_in_dx_calc=Input(rid="ri.vector.main.execute.71b347f9-e199-4653-a33a-ead6cedae191")
)
# STEP 7: add_alt_rollup ######################################################################################

from pyspark.sql import functions as F

# Output pyspark dataframe:  add_alt_rollup
# Input  pyspark dataframe:  pre_post_dx_more_in_post, final_rollups

def add_alt_rollup(final_rollups, pre_post_more_in_dx_calc):

    pre_post_dx_final = pre_post_more_in_dx_calc

    condition = [pre_post_dx_final['condition_concept_name'] == final_rollups['child_concept_name'] ]
    
    df = pre_post_dx_final.join(final_rollups.select(['child_concept_name', 'parent_concept_name']), how='left', on=condition)
    
    df = df.drop('child_concept_name')
    df = df.withColumnRenamed('parent_concept_name', 'high_level_condition')

    return df

@transform_pandas(
    Output(rid="ri.vector.main.execute.89c76680-b684-4139-8c4e-967cbb8b274c"),
    Feature_table_builder=Input(rid="ri.foundry.main.dataset.ce7a93a0-4140-4fdb-b97d-fb78c0caf345"),
    long_covid_patients=Input(rid="ri.foundry.main.dataset.34a5ed27-4c8c-49ae-b084-73bd73c79a49")
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
def add_labels(Feature_table_builder, long_covid_patients):

    # total patients

    # There are patients in the meds table that are not in the dx table and vice versa
    # This transform generates the full list of patients in either table (the union)
    # Along with the fields that are defined for every patient (sex, group, age etc)
    target_columns = ['person_id', 'sex', 'patient_group', 'apprx_age', 'race', 'ethn', 'tot_long_data_days', 
    'op_post_visit_ratio', 'post_ip_visit_ratio', "covid_ip_visit_ratio", "post_icu_visit_ratio", "covid_icu_visit_ratio", 'min_covid_dt', 'site_id']

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
    final_cols.extend(["long_covid", "hospitalized", 'date_encode', 'season_covid'])
    final_cols.remove('min_covid_dt')

    df = df.join(long_covid_patients, on='person_id', how='inner')
    
    # Join with the long covid clinic data to build our labels (long_covid)
    df = df.withColumn("long_covid", F.when(df["pasc_index"].isNotNull(), 1).otherwise(0))
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
    Output(rid="ri.foundry.main.dataset.a46f7b5a-55b6-4ac5-9ede-6fef340e53b4")
)
from pyspark.sql.types import *
def biological_pathways():
    schema = StructType([StructField("row_ind", StringType(), True), StructField("pathway", StringType(), True)])
    return spark.createDataFrame([["0","* demographics_anthro"],["1","   * apprx_age"],["2","   * sex_male"],["3","   * sexually_active"],["4","   * Obesity"],["5","   * Body Mass Index (BMI) [Ratio]"],["6","   * morbid_obesity"],["7","   * person_id"],["8","   * sex_unknown"],["9","   * race_american_indian_or_alaska_native"],["10","   * race_asian_or_pacific_islander"],["11","   * race_black_or_african_american"],["12","   * race_other"],["13","   * race_unknown"],["14","   * race_white"],["15","   * ethn_hispanic_or_latino"],["16","   * ethn_unknown"],["17","   * admits_alcohol_use"],["18","   * body_mass_index_40____severely_obese"],["19","   * current_every_day_smoker"],["20","   * denies_alcohol_use"],["21","   * divorced"],["22","   * former_smoker"],["23","   * living_with_partner"],["24","   * married"],["25","   * never_married"],["26","   * never_smoker"],["27","   * refused"],["28","   * separated"],["29","   * single"],["30","   * widowed"],["31","   * state"],["32","   * postal_code"],["33","   * TOBACCOSMOKER_before_or_day_of_covid_indicator"],["34","* visitation_procedures"],["35","   * Op_post_visit_ratio"],["36","   * Ip_post_visit_ratio"],["37","   * hospitalized"],["38","   * iopamidol"],["39","   * furosemide"],["40","   * atropine"],["41","   * gadopentetate_dimeglumine"],["42","   * tot_long_data_days"],["43","   * total"],["44","   * post_ip_visit_ratio"],["45","   * covid_ip_visit_ratio"],["46","   * post_icu_visit_ratio"],["47","   * covid_icu_visit_ratio"],["48","   * date_encode"],["49","   * season_covid"],["50","* respiratory"],["51","   * Inspired oxygen concentration"],["52","   * Respiratory rate"],["53","   * Oxygen saturation in Arterial blood by Pulse oximetry"],["54","   * Oxygen saturation in blood"],["55","   * difficulty_breathing"],["56","   * dyspnea"],["57","   * albuterol"],["58","   * fluticasone"],["59","   * palpitations"],["60","   * uncomplicated_asthma"],["61","   * formoterol"],["62","   * cough"],["63","   * interstitial_lung_disease"],["64","   * viral_lower_respiratory_infection"],["65","   * promethazine"],["66","   * asthma"],["67","   * lesion_of_lung"],["68","   * breathing_related_sleep_disorder"],["69","   * tracheobronchial_disorder"],["70","   * hypoxemia"],["71","   * lower_respiratory_infection_caused_by_sars_cov_2"],["72","   * chronic_sinusitis"],["73","   * pneumonia_caused_by_sars_cov_2"],["74","   * fibrosis_of_lung"],["75","   * acute_respiratory_disease"],["76","   * chronic_cough"],["77","   * benzonatate"],["78","   * hypoxemic_respiratory_failure"],["79","   * dependence_on_enabling_machine_or_device"],["80","   * dependence_on_respiratory_device"],["81","   * moderate_persistent_asthma"],["82","   * pulmonary_function_studies_abnormal"],["83","   * respiratory_obstruction"],["84","   * chronic_disease_of_respiratory_systemx"],["85","   * acute_respiratory_infections"],["86","   * guaifenesin"],["87","   * basic_nasal_oxygen_cannula"],["88","   * high_flow_oxygen_nasal_cannula"],["89","   * n3c_other_oxygen_device"],["90","   * ventilator"],["91","   * chronic_pulmonary_disease"],["92","   * PULMONARYEMBOLISM_before_or_day_of_covid_indicator"],["93","* Infection"],["94","   * clavulanate"],["95","   * amoxicillin"],["96","   * disorder_due_to_infection"],["97","   * ceftriaxone"],["98","   * influenza_b_virus_antigen"],["99","   * metronidazole"],["100","   * mupirocin"],["101","   * fluconazole"],["102","   * nitrofurantoin"],["103","* cardiovascular"],["104","      * Fibrin degradation products, D-dimer; quantitative"],["105","      * Fibrinogen [Mass/volume] in Platelet poor plasma by Coagulation assay"],["106","      * Platelets [#/volume] in Blood by Automated count"],["107","      * INR in Platelet poor plasma by Coagulation assay"],["108","      * Apixaban"],["109","      * long_term_current_use_of_anticoagulant"],["110","      * Heart rate"],["111","      * Systolic blood pressure"],["112","      * Diastolic blood pressure"],["113","      * Body temperature"],["114","      * Hemoglobin [mass/volume] in blood"],["115","      * tachycardia"],["116","      * metabolic_disease"],["117","      * chest_pain"],["118","      * vascular_disorder"],["119","      * spironolactone"],["120","      * heart_disease"],["121","      * cardiac_arrhythmia"],["122","      * hypertensive_disorder"],["123","      * tachyarrhythmia"],["124","      * myocarditis"],["125","      * losartan"],["126","      * disease_of_non_coronary_systemic_artery"],["127","      * congestive_heart_failure"],["128","      * CARDIOMYOPATHIES_before_or_day_of_covid_indicator"],["129","      * PERIPHERALVASCULARDISEASE_before_or_day_of_covid_indicator"],["130","      * HEARTFAILURE_before_or_day_of_covid_indicator"],["131","      * Troponin, quantitative "],["132","      * Creatine kinase [Enzymatic activity/volume] in Serum or Plasma"],["133","* female"],["134","   * complication_of_pregnancy__childbirth_and_or_the_puerperium"],["135","   * estradiol"],["136","   * ethinyl_estradiol"],["137","   * levonorgestrel"],["138","   * PREGNANCY_before_or_day_of_covid_indicator"],["139","* mental"],["140","   * Cortisol [Mass/volume] in Serum or Plasma "],["141","   * fatigue"],["142","   * mental_disorder"],["143","   * malaise"],["144","   * chronic_fatigue_syndrome"],["145","   * impaired_cognition"],["146","   * dyssomnia"],["147","   * anxiety_disorder"],["148","   * anxiety"],["149","   * loss_of_sense_of_smell"],["150","   * amitriptyline"],["151","   * sleep_disorder"],["152","   * neurosis"],["153","   * communication_disorder"],["154","   * amnesia"],["155","   * buspirone"],["156","   * memory_impairment"],["157","   * insomnia"],["158","   * dizziness"],["159","   * depressive_disorder"],["160","   * lorazepam"],["161","   * adjustment_disorder"],["162","   * epinephrine"],["163","   * major_depression__single_episode"],["164","   * gabapentin"],["165","   * sleep_apnea"],["166","   * cognitive_communication_disorder"],["167","   * dizziness_and_giddiness"],["168","   * duloxetine"],["169","   * bipolar_disorder"],["170","   * drug_related_disorder"],["171","   * nortriptyline"],["172","   * apnea"],["173","   * disorders_of_initiating_and_maintaining_sleep"],["174","   * SUBSTANCEABUSE_before_or_day_of_covid_indicator"],["175","* pain"],["176","   * Pain intensity rating scale"],["177","   * chronic_pain"],["178","   * muscle_pain"],["179","   * disorder_characterized_by_pain"],["180","   * pain_of_truncal_structure"],["181","   * neuropathy"],["182","   * shoulder_joint_pain"],["183","   * abdominal_pain"],["184","   * skin_sensation_disturbance"],["185","   * ketorolac"],["186","   * pain_in_lower_limb"],["187","   * spondylosis"],["188","   * vascular_headache"],["189","   * shoulder_pain"],["190","   * pain"],["191","   * headache_disorder"],["192","   * migraine"],["193","   * headache"],["194","   * morphine"],["195","   * seborrheic_keratosis"],["196","   * effusion_of_joint"],["197","   * seborrheic_dermatitis"],["198","   * aspirin"],["199","   * acetaminophen"],["200","   * oxycodone"],["201","   * knee_pain"],["202","   * backache"],["203","   * fentanyl"],["204","   * pain_in_upper_limb"],["205","   * neck_pain"],["206","* digestive"],["207","   * diarrhea"],["208","   * acute_digestive_system_disorder"],["209","   * gastroesophageal_reflux_disease_without_esophagitis"],["210","   * pantoprazole"],["211","   * omeprazole"],["212","   * altered_bowel_function"],["213","* inflammation"],["214","   * IL-6 "],["215","   * inflammation_of_specific_body_organs"],["216","   * inflammation_of_specific_body_systems"],["217","   * prednisone"],["218","   * inflammation_of_specific_body_structures_or_tissue"],["219","   * meloxicam"],["220","   * Antinuclear antibody (ANA) "],["221","   * connective_tissue_disorder_by_body_site"],["222","   * hydrocortisone"],["223","   * collagen_disease"],["224","   * allergic_rhinitis"],["225","   * allergic_disposition"],["226","   * triamcinolone"],["227","   * loratadine"],["228","   * interleukin_6__mass_volume__in_serum_or_plasma"],["229","   * fever"],["230","   * SOLIDORGANORBLOODSTEMCELLTRANSPLANT_before_or_day_of_covid_indicator"],["231","   * OTHERIMMUNOCOMPROMISED_before_or_day_of_covid_indicator"],["232","   * SYSTEMICCORTICOSTEROIDS_before_or_day_of_covid_indicator"],["233","* renal_liver"],["234","      * dulaglutide"],["235","      * prediabetes"],["236","      * metformin"],["237","      * diabetes"],["238","      * long_term_current_use_of_insulin"],["239","      * Creatinine renal clearance predicted by Cockcroft-Gault formula "],["240","      * Creatinine [Mass/volume] in Serum or Plasma "],["241","      * Glomerular filtration rate/1.73 sq M.predicted [Volume Rate/Area] in Serum, Plasma or Blood by Creatinine-based formula (MDRD) "],["242","      * chronic_kidney_disease_due_to_hypertension"],["243","      * kidney_disease"],["244","      * chronic_kidney_disease_due_to_type_2_diabetes_mellitus"],["245","      * Budesonide"],["246","      * dependence_on_renal_dialysis"],["247","      * creatinine__blood"],["248","      * Alanine aminotransferase [Enzymatic activity/volume] in Serum or Plasma"],["249","      * MILDLIVERDISEASE_before_or_day_of_covid_indicator"],["250","      * MODERATESEVERELIVERDISEASE_before_or_day_of_covid_indicator"],["251","* nutrition "],["252","   * Vitamin D "],["253","   * Potassium [Moles/volume] in serum or plasma"],["254","   * Sodium [Moles/volume] in serum or plasma"],["255","   * Calcium [Mass/volume] in serum or plasma"],["256","   * Glucose [mass/volume] in Serum or Plasma"],["257","   * deficiency_of_micronutrients"],["258","   * iron_deficiency_anemia_due_to_blood_loss"],["259","   * hyperlipidemia"],["260","   * mineral_deficiency"],["261","   * cholecalciferol"],["262","   * rbc_count_low"],["263","   * sodium_chloride"],["264","   * vitamin_disease"],["265","   * pure_hypercholesterolemia"],["266","   * sodium_acetate"],["267","   * hemoglobin_low"],["268","   * mixed_hyperlipidemia"],["269","   * vitamin_b12"],["270","   * hypercholesterolemia"],["271","   * potassium_chloride"],["272","* covid"],["273","   * SARS-CoV-2 (COVID-19) RNA [Presence] in Respiratory specimen by NAA with probe detection "],["274","   * SARS-CoV-2 (COVID-19) RNA [Presence] in Specimen by NAA with probe detection "],["275","   * SARS-CoV-2 (COVID-19) N gene [Presence] in Specimen by Nucleic acid amplification using CDC primer-probe set N1 "],["276","   * SARS-CoV-2 (COVID-19) ORF1ab region [Presence] in Respiratory specimen by NAA with probe detection"],["277","   * SARS-CoV-2 (COVID-19) Ag [Presence] in Respiratory specimen by Rapid immunoassay "],["278","   * SARS-CoV-2 (COVID-19) RdRp gene [Presence] in Respiratory specimen by NAA with probe detection "],["279","   * SARS-CoV-2 (COVID-19) RdRp gene [Presence] in Specimen by NAA with probe detection "],["280","   * SARS-CoV+SARS-CoV-2 (COVID-19) Ag [Presence] in Respiratory specimen by Rapid immunoassay "],["281","   * SARS-CoV-2 (COVID-19) RNA panel - Specimen by NAA with probe detection"],["282","   * SARS-CoV-2 (COVID-19) RNA [Presence] in Nasopharynx by NAA with non-probe detection "],["283","   * SARS-CoV-2 (COVID-19) N gene [Presence] in Specimen by NAA with probe detection "],["284","   * SARS-CoV-2 (COVID-19) IgG Ab [Presence] in Serum or Plasma by Immunoassay "],["285","   * sars_cov_2__covid_19__vaccine__mrna_spike_protein"],["286","   * long_covid"],["287","   * Severity_Type"],["288","   * REMDISIVIR_during_covid_hospitalization_indicator"],["289","   * COVIDREGIMENCORTICOSTEROIDS_during_covid_hospitalization_indicator"],["290","   * number_of_COVID_vaccine_doses_before_or_day_of_covid"],["291","   * Antibody_Neg_before_or_day_of_covid_indicator"],["292","* ageing_general"],["293","   * blood_chemistry_abnormal"],["294","   * abnormal_presence_of_protein"],["295","   * bleeding"],["296","   * degenerative_disorder"],["297","   * chronic_nervous_system_disorder"],["298","   * osteoporosis"],["299","   * traumatic_and_or_non_traumatic_injury_of_anatomical_site"],["300","   * colchicine"],["301","   * anomaly_of_eye"],["302","   * osteoarthritis"],["303","   * genitourinary_tract_hemorrhage"],["304","   * genuine_stress_incontinence"],["305","   * traumatic_and_or_non_traumatic_injury"],["306","   * involuntary_movement"],["307","   * peripheral_nerve_disease"],["308","   * injury_of_free_lower_limb"],["309","   * glaucoma"],["310","   * radiology_result_abnormal"],["311","   * mass_of_soft_tissue"],["312","   * spinal_stenosis_of_lumbar_region"],["313","   * cyst"],["314","   * soft_tissue_lesion"],["315","   * skin_lesion"],["316","   * arthropathy"],["317","   * soft_tissue_injury"],["318","   * cytopenia"],["319","   * RHEUMATOLOGICDISEASE_before_or_day_of_covid_indicator"]], schema=schema)

@transform_pandas(
    Output(rid="ri.vector.main.execute.aa067352-f27e-4d5d-9b1b-a7a53183c28b"),
    concept=Input(rid="ri.foundry.main.dataset.5cb3c4a3-327a-47bf-a8bf-daf0cafe6772"),
    concept_ancestor=Input(rid="ri.foundry.main.dataset.c5e0521a-147e-4608-b71e-8f53bcdbe03c"),
    long_covid_patients=Input(rid="ri.foundry.main.dataset.34a5ed27-4c8c-49ae-b084-73bd73c79a49"),
    pre_post_dx_count_clean=Input(rid="ri.foundry.main.dataset.993922b2-c5f2-4508-9ec7-b25aaa0c9750")
)
# STEP 4:  condition_rollup #####################################################################################

# Output pyspark dataframe:  condition_rollup
# Input  pyspark dataframe:  long_covid_patients, pre_post_dx_count_clean, concept_ancestor, concept
# NOTE: concept and concept_ancestor are standard OMOP vocabulary tables

def condition_rollup(pre_post_dx_count_clean, concept_ancestor, concept, long_covid_patients):
   
    pp = pre_post_dx_count_clean.alias('pp')
    ca = concept_ancestor.alias('ca')
    ct = concept.alias('ct')
    lc = long_covid_patients.alias('lc')

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
    Output(rid="ri.vector.main.execute.fbb15d5f-bb7b-426f-9056-2bf0fa4b068d"),
    pre_post_dx_final=Input(rid="ri.vector.main.execute.435bf8d5-1d90-4bce-9864-2aee8744aad7")
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
    Output(rid="ri.foundry.main.dataset.1ef41f96-7291-4a0b-9cd7-1d7e24a5a5d8"),
    Comorbidity_counts=Input(rid="ri.foundry.main.dataset.2513e4c2-bba0-4067-843f-dec2dfa2b858"),
    Covid_measures=Input(rid="ri.foundry.main.dataset.5b072ea2-72c8-42d1-b653-1cfaf691857b"),
    Device_count_clean=Input(rid="ri.foundry.main.dataset.3dedc0a1-f9dc-441c-9f1c-eee2ee5176ba"),
    Lab_measures_clean=Input(rid="ri.foundry.main.dataset.b6344895-930e-4c2c-a065-2078d950abff"),
    Obs_person_pivot=Input(rid="ri.foundry.main.dataset.bf7621a4-7d12-4846-99bd-61b518242b14"),
    add_labels=Input(rid="ri.vector.main.execute.89c76680-b684-4139-8c4e-967cbb8b274c"),
    count_dx_pre_and_post=Input(rid="ri.vector.main.execute.fbb15d5f-bb7b-426f-9056-2bf0fa4b068d"),
    features=Input(rid="ri.foundry.main.dataset.02867ee7-ced4-4d6f-913f-1d81785e66fb"),
    pre_post_dx_final=Input(rid="ri.vector.main.execute.435bf8d5-1d90-4bce-9864-2aee8744aad7"),
    pre_post_med_final_distinct=Input(rid="ri.vector.main.execute.90ed968a-8e05-4075-80b8-8c485bcb989a"),
    severity_table=Input(rid="ri.vector.main.execute.158df43a-aa01-4c5a-be09-bcf738387305")
)
 # STEP 10: feature_table_all_patients ######################################################################################

# Output pyspark dataframe:  feature_table_all_patients
# Input  pyspark dataframe:  training_data, add_labels, pre_post_med_final, pre_post_dx_final, count_dx_pre_and_post

def feature_table_all_patients_dx_drug(features, add_labels, pre_post_dx_final, count_dx_pre_and_post, pre_post_med_final_distinct, Comorbidity_counts, Lab_measures_clean, Covid_measures, Device_count_clean,  Obs_person_pivot, severity_table):

    # Filter only to meds used in the canonical all patients model and then pivot
    features_p = features.toPandas()
    f=features_p['features'].tolist()
    med_df = pivot_meds(pre_post_med_final_distinct, f)
    
    dx_df = pivot_dx(pre_post_dx_final, f)
    lab_df = pivot_measure(Lab_measures_clean)
    covid_df = pivot_covid(Covid_measures)

    # nlp_df = pivot_nlp(Nlp_count_clean)
    device_df = pivot_device(Device_count_clean)

    result = build_final_feature_table(med_df, dx_df, add_labels, count_dx_pre_and_post, lab_df, covid_df, device_df)
    result = result.join(Comorbidity_counts, on='person_id', how='left')
    result = result.join(Obs_person_pivot, on='person_id', how='left')
    
    result = result.join(severity_table, on = 'person_id', how='left')
    result = result.fillna(0)
    # print(result.count())

    return result
    

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.02867ee7-ced4-4d6f-913f-1d81785e66fb")
)
from pyspark.sql.types import *
def features():
    schema = StructType([StructField("features", StringType(), True)])
    return spark.createDataFrame([["difficulty_breathing"],["apprx_age"],["fatigue"],["op_post_visit_ratio"],["dyspnea"],["ip_post_visit_ratio"],["albuterol"],["hospitalized"],["sex_male"],["fluticasone"],["palpitations"],["mental_disorder"],["uncomplicated_asthma"],["chronic_pain"],["malaise"],["chronic_fatigue_syndrome"],["formoterol"],["tachycardia"],["metabolic_disease"],["chest_pain"],["inflammation_of_specific_body_organs"],["impaired_cognition"],["diarrhea"],["acetaminophen"],["dyssomnia"],["anxiety_disorder"],["cough"],["anxiety"],["muscle_pain"],["interstitial_lung_disease"],["migraine"],["degenerative_disorder"],["viral_lower_respiratory_infection"],["promethazine"],["deficiency_of_micronutrients"],["asthma"],["disorder_characterized_by_pain"],["apixaban"],["lesion_of_lung"],["inflammation_of_specific_body_systems"],["breathing_related_sleep_disorder"],["chronic_nervous_system_disorder"],["iopamidol"],["loss_of_sense_of_smell"],["amitriptyline"],["sleep_disorder"],["pain_of_truncal_structure"],["neurosis"],["headache"],["tracheobronchial_disorder"],["communication_disorder"],["amnesia"],["hypoxemia"],["lower_respiratory_infection_caused_by_sars_cov_2"],["bleeding"],["amoxicillin"],["disorder_due_to_infection"],["chronic_sinusitis"],["pain_in_lower_limb"],["furosemide"],["buspirone"],["vascular_disorder"],["memory_impairment"],["insomnia"],["budesonide"],["prednisone"],["pneumonia_caused_by_sars_cov_2"],["clavulanate"],["dizziness"],["neuropathy"],["iron_deficiency_anemia_due_to_blood_loss"],["estradiol"],["ceftriaxone"],["shoulder_joint_pain"],["sexually_active"],["abdominal_pain"],["skin_sensation_disturbance"],["ketorolac"],["depressive_disorder"],["hyperlipidemia"],["chronic_kidney_disease_due_to_hypertension"],["spondylosis"],["vascular_headache"],["fibrosis_of_lung"],["acute_respiratory_disease"],["chronic_cough"],["osteoporosis"],["lorazepam"],["connective_tissue_disorder_by_body_site"],["adjustment_disorder"],["benzonatate"],["shoulder_pain"],["mineral_deficiency"],["obesity"],["epinephrine"],["dependence_on_enabling_machine_or_device"],["dependence_on_respiratory_device"],["inflammation_of_specific_body_structures_or_tissue"],["spironolactone"],["cholecalciferol"],["heart_disease"],["pain"],["major_depression__single_episode"],["meloxicam"],["hydrocortisone"],["collagen_disease"],["headache_disorder"],["hypoxemic_respiratory_failure"],["morphine"],["cardiac_arrhythmia"],["seborrheic_keratosis"],["gabapentin"],["dulaglutide"],["hypertensive_disorder"],["effusion_of_joint"],["moderate_persistent_asthma"],["morbid_obesity"],["seborrheic_dermatitis"],["rbc_count_low"],["blood_chemistry_abnormal"],["acute_digestive_system_disorder"],["sars_cov_2__covid_19__vaccine__mrna_spike_protein"],["influenza_b_virus_antigen"],["pulmonary_function_studies_abnormal"],["sleep_apnea"],["abnormal_presence_of_protein"],["sodium_chloride"],["atropine"],["aspirin"],["cognitive_communication_disorder"],["metronidazole"],["ethinyl_estradiol"],["gadopentetate_dimeglumine"],["traumatic_and_or_non_traumatic_injury_of_anatomical_site"],["colchicine"],["anomaly_of_eye"],["oxycodone"],["osteoarthritis"],["complication_of_pregnancy__childbirth_and_or_the_puerperium"],["allergic_rhinitis"],["dizziness_and_giddiness"],["genitourinary_tract_hemorrhage"],["duloxetine"],["bipolar_disorder"],["vitamin_disease"],["respiratory_obstruction"],["genuine_stress_incontinence"],["chronic_disease_of_respiratory_systemx"],["traumatic_and_or_non_traumatic_injury"],["drug_related_disorder"],["nortriptyline"],["involuntary_movement"],["knee_pain"],["peripheral_nerve_disease"],["gastroesophageal_reflux_disease_without_esophagitis"],["mupirocin"],["fluconazole"],["pure_hypercholesterolemia"],["kidney_disease"],["injury_of_free_lower_limb"],["glaucoma"],["backache"],["tachyarrhythmia"],["myocarditis"],["nitrofurantoin"],["prediabetes"],["sodium_acetate"],["apnea"],["losartan"],["radiology_result_abnormal"],["pantoprazole"],["hemoglobin_low"],["mixed_hyperlipidemia"],["mass_of_soft_tissue"],["levonorgestrel"],["omeprazole"],["allergic_disposition"],["metformin"],["fentanyl"],["spinal_stenosis_of_lumbar_region"],["cyst"],["soft_tissue_lesion"],["altered_bowel_function"],["skin_lesion"],["triamcinolone"],["pain_in_upper_limb"],["acute_respiratory_infections"],["neck_pain"],["guaifenesin"],["disorders_of_initiating_and_maintaining_sleep"],["loratadine"],["vitamin_b12"],["hypercholesterolemia"],["potassium_chloride"],["arthropathy"],["chronic_kidney_disease_due_to_type_2_diabetes_mellitus"],["disease_of_non_coronary_systemic_artery"],["soft_tissue_injury"],["cytopenia"],["fever"]], schema=schema)

@transform_pandas(
    Output(rid="ri.vector.main.execute.4d979157-b9d2-4467-9ff5-652a17ef76a3"),
    condition_rollup=Input(rid="ri.vector.main.execute.aa067352-f27e-4d5d-9b1b-a7a53183c28b"),
    parent_condition_rollup=Input(rid="ri.vector.main.execute.c529efa8-86ea-415f-8498-8c1725c5b9a5")
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
    Output(rid="ri.foundry.main.dataset.6f1d6d9a-9bc8-437c-8a4b-ce02d6ec5ef4"),
    Comorbidity_counts=Input(rid="ri.foundry.main.dataset.2513e4c2-bba0-4067-843f-dec2dfa2b858"),
    Covid_measures=Input(rid="ri.foundry.main.dataset.5b072ea2-72c8-42d1-b653-1cfaf691857b"),
    Lab_measures_clean=Input(rid="ri.foundry.main.dataset.b6344895-930e-4c2c-a065-2078d950abff"),
    Obs_person_pivot=Input(rid="ri.foundry.main.dataset.bf7621a4-7d12-4846-99bd-61b518242b14"),
    add_labels=Input(rid="ri.vector.main.execute.89c76680-b684-4139-8c4e-967cbb8b274c"),
    biological_pathways=Input(rid="ri.foundry.main.dataset.a46f7b5a-55b6-4ac5-9ede-6fef340e53b4"),
    feature_table_all_patients_dx_drug=Input(rid="ri.foundry.main.dataset.1ef41f96-7291-4a0b-9cd7-1d7e24a5a5d8"),
    pre_post_dx_final=Input(rid="ri.vector.main.execute.435bf8d5-1d90-4bce-9864-2aee8744aad7"),
    pre_post_med_final_distinct=Input(rid="ri.vector.main.execute.90ed968a-8e05-4075-80b8-8c485bcb989a"),
    severity_table=Input(rid="ri.vector.main.execute.158df43a-aa01-4c5a-be09-bcf738387305")
)
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType,StructField, StringType
from pyspark.sql.functions import udf
from pyspark.sql import functions as F
import re
import pandas as pd

def metatable(feature_table_all_patients_dx_drug, Lab_measures_clean, Covid_measures, pre_post_med_final_distinct, pre_post_dx_final, add_labels, Comorbidity_counts, Obs_person_pivot, severity_table, biological_pathways):
    df = feature_table_all_patients_dx_drug
    info = df.dtypes

    standardize = lambda l: [re.sub("[^A-Za-z_0-9]", "_", x).lower() for x in l]
    name_c = ['measurement_concept_name', 'measure_type']
    col = df.columns
    l_comor = Comorbidity_counts.columns
    l_obs = Obs_person_pivot.columns
    l_basic = add_labels.columns
    l_covid = standardize(list(Covid_measures.select('measure_type').distinct().toPandas()['measure_type']))
    l_severity = severity_table.columns

    # add formatted colummns for variables with high importance
    formatted_map = {"tot_long_data_days": "Follow up time (days)", "total_covid_dx": "Macrovists (COVID, dx)",
                "site_id_124": "Data partner 124",
                "viral_lower_respiratory_infection_c_dx": "Viral lower respiratory infection (COVID)", 
                "apprx_age": "Age (years)",
                "total_pre_pre_dx": "Macrovisits (total number, baseline)",
                "site_id_399": "Data partner 399",
                "never_smoker": "Never smoker",
                "total_post_dx": "Macrovisits (total number, post-COVID)",
                "site_id_569": "Data partner 569",
                "date_encode": "Index COVID date (month)",
                "state_NULL": "State (missing)",
                "site_id_134": "Data partner 134",
                "hospitalized": "Hospitalization",
                "op_post_visit_ratio": "Outpatient visitation rate (post-COVID)",
                "SYSTEMICCORTICOSTEROIDS_before_or_day_of_covid_indicator": "Systemic corticosteroid use (pre-COVID)",
                "albuterol_covid_med": "Albuterol use (COVID)"}

    bdf = biological_pathways.toPandas()    
    bdf['row_ind'] = bdf['row_ind'].astype(int)
    maps = {}
    for index in range(bdf.shape[0]):
        #word = bdf.loc[bdf['row_ind'] == index, 'pathway']
        word = bdf[bdf['row_ind']==index]['pathway'].item()
        if word[0] == '*': 
            k = word[2:]
            maps[k] = []
        else:
            if len(maps)==0: continue
            v = word.lstrip(" *")
            maps[k].append(v)

    categories = [''] * len(col)
    pathways = [''] * len(col)
    temps = [''] * len(col)
    formats = [''] * len(col)
    for idx, c in enumerate(col):
        # define temporal components
        if '_post' in c: temps[idx] = 'post covid'
        elif '_pp' in c: temps[idx] = 'pre pre covid'
        elif '_pre' in c: temps[idx] = 'pre covid'
        elif '_covid' in c or 'c_dx' in c: temps[idx] = 'covid' 
        else: temps[idx] = 'pre pre covid'

        # define biological pathways
        for k, v in maps.items():
            t_list = standardize(v)
            c_standardize = standardize([c])[0]
            if any(s in c_standardize for s in t_list):
            #if any(s in c for s in v):
                pathways[idx] = k
        
        # define formatted variables:
        for k, v in formatted_map.items():
            if k == c:
                formats[idx] = v

        # define types
        if 'dx' in c: categories[idx] = 'diagnosis'
        elif 'med' in c: categories[idx] = 'medication'
        elif 'device' in c: categories[idx] = 'device'
        elif any(s in c for s in l_basic):
            categories[idx] = 'basic'
        elif any(s in c for s in l_covid):
            categories[idx] = 'covid'
        elif any(s in c for s in l_comor):
            categories[idx] = 'comorbility'
        elif any(s in c for s in l_obs):
            categories[idx] = 'observation'
        elif any(s in c for s in l_severity):
            categories[idx] = 'severity'
        else:
            categories[idx] = 'lab'

        # define temporal components
        if '_post' in c: temps[idx] = 'post covid'
        elif '_pp' in c: temps[idx] = 'pre pre covid'
        elif '_pre' in c: temps[idx] = 'pre covid'
        elif '_covid' in c or 'c_dx' in c: temps[idx] = 'covid' 
        else: temps[idx] = 'pre pre covid'

    columns = ["Name", "Type", "Category", "Pathways", 'Temporal', 'Formatted']
    dat = [ t + (ca, ) + (p, ) + (te, ) + (f, ) for t, ca, p, te, f in zip(info, categories, pathways, temps, formats)]

        
    spark = SparkSession.builder.appName('sparkdf').getOrCreate()
    table = spark.createDataFrame(dat, columns)
    # table = table.withColumn('Category', F.lit(categories))

    return table

@transform_pandas(
    Output(rid="ri.vector.main.execute.c529efa8-86ea-415f-8498-8c1725c5b9a5"),
    condition_rollup=Input(rid="ri.vector.main.execute.aa067352-f27e-4d5d-9b1b-a7a53183c28b")
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
    Output(rid="ri.vector.main.execute.435bf8d5-1d90-4bce-9864-2aee8744aad7"),
    add_alt_rollup=Input(rid="ri.vector.main.execute.2f844f9b-376d-41a0-84f9-9809f3fc0b80")
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
    Output(rid="ri.vector.main.execute.90ed968a-8e05-4075-80b8-8c485bcb989a"),
    pre_post_med_count_clean=Input(rid="ri.foundry.main.dataset.fa3fe17e-58ac-4615-a238-0fc24ecd9b6e")
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
    Output(rid="ri.vector.main.execute.71b347f9-e199-4653-a33a-ead6cedae191"),
    pre_post_dx_count_clean=Input(rid="ri.foundry.main.dataset.993922b2-c5f2-4508-9ec7-b25aaa0c9750")
)
# STEP 3:  pre_post_more_in_dx_calc #####################################################################################

# Output pyspark dataframe:  pre_post_dx_more_in_post_calc
# Input  pyspark dataframe:  pre_post_dx

def pre_post_more_in_dx_calc(pre_post_dx_count_clean):
    
    result = pre_post_dx_count_clean
    # result = result.withColumn('post_only_dx', 
    #                             F.when(result['pre_dx_count'] > 0, 0)
    #                             .otherwise(1)
    #                           )
    
    # result = result.withColumn('more_in_post',
    #                             F.when(result['post_dx_count'] > result['pre_dx_count'], 1)
    #                             .otherwise(0)
    #                           )

    
    return result
    

@transform_pandas(
    Output(rid="ri.vector.main.execute.158df43a-aa01-4c5a-be09-bcf738387305"),
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
    df = df.withColumn('state', F.when((df["state"].isNull()) | (df['state']==0), 'NULL').when(df['state']=='MAINE', 'ME').otherwise(df['state']))
    df = df.select(targets).toPandas()
    result = pd.get_dummies(df, columns=["Severity_Type", "state"])
    result = spark.createDataFrame(result)
    return result

    

