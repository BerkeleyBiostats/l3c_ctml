from pyspark.sql.functions import *


def sql_statement_00(Feature_table_builder, high_level_condition_occur):
    feat = Feature_table_builder
    hlc = high_level_condition_occur
    comorbidity_counts = feat.join(hlc, "person_id", "left") \
        .groupBy("person_id", "patient_group", "high_level_condition") \
        .agg(countDistinct("high_level_condition").alias("comorbidity_count")) \
        .select("person_id", "patient_group", "high_level_condition", "comorbidity_count")

    comorbidity_count_pivot = (
        comorbidity_counts.groupBy("person_id", "patient_group")
            .pivot("high_level_condition",
                   ["diabetes", "prediabetes", "diabetes_complications", "chronic_kidney_disease",
                    "congestive_heart_failure", "chronic_pulmonary_disease"])
            .sum("comorbidity_count").alias("count"))
    result = comorbidity_count_pivot.na.fill(0)
    return result


def sql_statement_01(Feature_table_builder, condition_occurrence, concept):
    diabete_l = ['Type 2 diabetes mellitus',
                 'Type 2 diabetes mellitus without complication',
                 'Type 1 diabetes mellitus without complication',
                 'Type 1 diabetes mellitus uncontrolled',
                 'Type 1 diabetes mellitus',
                 'Diabetes mellitus without complication']

    diabete_cl = ['Renal disorder due to type 2 diabetes mellitus',
                  'Disorder of kidney due to diabetes mellitus',
                  'Disorder due to type 2 diabetes mellitus',
                  'Disorder of nervous system due to type 2 diabetes mellitus',
                  'Polyneuropathy due to type 2 diabetes mellitus',
                  'Hyperglycemia due to type 2 diabetes mellitus',
                  'Complication due to diabetes mellitus',
                  'Peripheral circulatory disorder due to type 2 diabetes mellitus',
                  'Autonomic neuropathy due to type 2 diabetes mellitus',
                  'Hyperglycemia due to type 1 diabetes mellitus',
                  'Foot ulcer due to type 2 diabetes mellitus',
                  'Disorder of eye due to type 2 diabetes mellitus',
                  'Hypoglycemia due to type 2 diabetes mellitus']

    kidney_l = ['Chronic kidney disease stage 3',
                'Chronic kidney disease',
                'Chronic kidney disease due to type 2 diabetes mellitus',
                'Chronic kidney disease due to hypertension',
                'Chronic kidney disease stage 3B',
                'Chronic kidney disease stage 4',
                'Chronic kidney disease stage 2',
                'Hypertensive heart and chronic kidney disease',
                'Anemia in chronic kidney disease',
                'Chronic kidney disease stage 5',
                'Chronic kidney disease stage 3A',
                'Chronic kidney disease stage 1']

    heart_l = ['Congestive heart failure', 'Heart failure', 'Systolic heart failure',
               'Hypertensive heart failure',
               'Chronic diastolic heart failure',
               'Chronic combined systolic and diastolic heart failure',
               'Chronic systolic heart failure',
               'Chronic congestive heart failure',
               'Acute on chronic systolic heart failure',
               'Diastolic heart failure',
               'Acute on chronic diastolic heart failure',
               'Hypertensive heart disease without congestive heart failure',
               'Biventricular congestive heart failure',
               'Acute systolic heart failure',
               'Fetal heart finding',
               'Acute on chronic combined systolic and diastolic heart failure',
               'Acute right-sided heart failure',
               'Left heart failure',
               'Hypertensive heart and renal disease with (congestive) heart failure',
               'Acute combined systolic and diastolic heart failure']

    pul_l = ['Chronic pulmonary edema',
             'Chronic pulmonary embolism',
             'Chronic pulmonary coccidioidomycosis',
             'Chronic obstructive pulmonary disease with acute lower respiratory infection',
             'Chronic cor pulmonale']
    feat = Feature_table_builder
    glc = condition_occurrence.join(concept, (condition_occurrence.condition_concept_id == concept.concept_id), 'left') \
        .select(
        col("person_id"),
        col("condition_occurrence_id"),
        col("condition_end_date"),
        col("condition_start_date"),
        col("concept_name"),
        when(col("concept_name").isin(['Prediabetes']), 'Prediabetes')
            .when(col("concept_name").isin(diabete_l), 'diabetes') \
            .when(col("concept_name").isin(diabete_cl), 'diabetes_complications') \
            .when(col("concept_name").isin(kidney_l), 'chronic_kidney_disease') \
            .when(col("concept_name").isin(heart_l), 'congestive_heart_failure') \
            .when(col("concept_name").isin(pul_l), 'chronic_pulmonary_disease') \
            .otherwise(None)
            .alias("high_level_condition"))

    result = feat.join(
        glc,
        (feat.person_id == glc.person_id) & (glc.condition_start_date <= feat.pre_window_end_dt),
        "inner"
    ).select(
        feat.person_id,
        feat.pre_window_start_dt,
        feat.pre_window_end_dt,
        col("high_level_condition"),
        glc.condition_start_date,
        glc.condition_end_date
    ).filter(
        (col("high_level_condition").isNotNull()) &
        ((col("condition_end_date") >= col("pre_window_start_dt")) | (col("condition_end_date").isNull()))
    )

    return result
