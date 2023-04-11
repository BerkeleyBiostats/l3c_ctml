import pandas as pd
import re

from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType
from pyspark.sql.types import *
from pyspark.sql.functions import udf
from pyspark.context import SparkContext
ctx = SparkContext.getOrCreate()


#=========================================Global functions==================================================
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




#=========================================combine data==================================================
def condition_rollup(long_covid_patients, pre_post_dx_count_clean, concept):
   
    pp = pre_post_dx_count_clean.alias('pp')
    # ca = concept_ancestor.alias('ca')
    ct = concept.alias('ct')
    lc = long_covid_patients.alias('lc')

    df = pp.join(lc, on='person_id', how='inner')
    df = df.join(ct, on=[df.condition_concept_id  == ct.concept_id], how='inner')

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

    #for gc in generic_codes:
        #df = df.filter( ~F.lower(ct.concept_name).like('%' + gc + '%') )
        
        #if gc not in ['right', 'left']:
            #df = df.filter( ~F.lower(pp.condition_concept_name).like('%' + gc + '%') )

    # df = df.filter(ca.min_levels_of_separation.between(0,2))


    
    df = df.groupby(['ct.concept_name', 'pp.condition_concept_id']).agg(F.countDistinct('pp.person_id').alias('ptct_training'))

    df = df.withColumnRenamed('concept_name', 'concept_name')
    # df = df.withColumnRenamed('condition_concept_name', 'child_concept_name')
    df = df.withColumnRenamed('min_levels_of_separation', 'min_hops_bt_parent_child')
    df = df.withColumnRenamed('max_levels_of_separation', 'max_hops_bt_parent_child')
    df = df.withColumnRenamed('condition_concept_id', 'concept_id')

    return df


def parent_condition_rollup(condition_rollup):
    df = condition_rollup
    df = df.groupby('concept_name').agg(F.sum('ptct_training').alias('total_pts'))
    df = df[df['total_pts'] >= 3]
    
    return df




def final_rollups(condition_rollup, parent_condition_rollup):
    pc = parent_condition_rollup
    dm = condition_rollup

    df = pc.join(dm, on='concept_name', how='inner')
    df = df.select(['concept_name', 'concept_id']).distinct()

    return df


def pre_post_more_in_dx_calc(pre_post_dx_count_clean):
    
    result = pre_post_dx_count_clean
    
    return result

















