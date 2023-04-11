from pyspark.sql.functions import *
from pyspark.sql.types import IntegerType

import pandas as pd


def get_concept_id(concept, s):
    concepts_df = concept \
        .select('concept_set_name',
                'is_most_recent_version', 'concept_id') \
        .where(col('is_most_recent_version') == 'true')

    ids = list(concepts_df.where(
        (concepts_df.concept_set_name == s)
        & (concepts_df.is_most_recent_version == 'true')
    ).select('concept_id').toPandas()['concept_id'])
    return ids


def get_dataframe(df, col_name, ids):
    # patient who do not have 
    sub_df = df.where(col('harmonized_value_as_number').isNotNull()) \
        .withColumn(col_name, when(df.measurement_concept_id.isin
                                   (ids), df.harmonized_value_as_number).otherwise(0))

    # aggregate by person and date
    sub_df = sub_df.groupby('person_id', 'visit_date').agg(
        max(col_name).alias(col_name))
    return sub_df


###@transform_pandas(
###    Output(rid="ri.vector.main.execute.b6af7e97-05b8-4768-9552-1e4046b18cf0"),
###    Feature_table_builder=Input(rid="ri.foundry.main.dataset.ce7a93a0-4140-4fdb-b97d-fb78c0caf345"),
###    measurement=Input(rid="ri.foundry.main.dataset.5c8b84fb-814b-4ee5-a89a-9525f4a617c7")
# )
def measurement_person(measurement, Feature_table_builder, concept):
    # select measurements between dates
    targets = ['Respiratory rate', 'Oxygen saturation in Arterial blood by Pulse oximetry',
               'Oxygen saturation in blood', 'Heart rate', 'Systolic blood pressure',
               'Diastolic blood pressure', 'Body temperature', 'Glucose [mass/volume] in Serum or Plasma',
               'Body Mass Index (BMI) [Ratio]', 'Hemoglobin [mass/volume] in blood',
               'Potassium [Moles/volume] in serum or plasma', 'Sodium [Moles/volume] in serum or plasma',
               'Calcium [Mass/volume] in serum or plasma',
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
               'Creatinine; blood']  # there are 32

    persons = Feature_table_builder.select('person_id')

    df = measurement.join(concept, (measurement.measurement_concept_id == concept.concept_id), 'left') \
        .select('person_id', 'measurement_date', 'measurement_concept_id', 'value_as_number', 'value_as_concept_id',
                'concept_name') \
        .where(col('measurement_date').isNotNull()) \
        .where(col('concept_name').isin(targets)) \
        .where(col('value_as_number').isNotNull()) \
        .withColumnRenamed('measurement_date', 'visit_date') \
        .join(persons, 'person_id', 'inner')

    return df


def sql_statement_00(feature_table_builder, measurement_person):
    df = feature_table_builder.alias("feat").join(measurement_person.alias("m"),
                                                  (col("feat.person_id") == col("m.person_id")) & (
                                                      col("m.visit_date").between(col("feat.pre_window_end_dt"),
                                                                                  col("feat.post_window_start_dt"))))

    result = df.groupBy("feat.person_id", "m.concept_name", "m.measurement_concept_id") \
        .agg(max("m.value_as_number").alias("max_measure"),
             min("m.value_as_number").alias("min_measure"),
             avg("m.value_as_number").alias("avg_measure"))
    return result


def sql_statement_01(covid_measurement, post_measurement, pre_measurement, pre_pre_measurement):
    ppd, pd, cd, pod = pre_pre_measurement, pre_measurement, covid_measurement, post_measurement
    k = ['person_id', 'concept_name', 'measurement_concept_id']
    dfs = [ppd, pd, cd, pod]
    labels = ['pre_pre_', 'pre_', 'covid_', 'max_']
    for i in range(4):
        df = dfs[i]
        s = labels[i]
        dfs[i] = df.withColumnRenamed('max_measure', s + 'max').withColumnRenamed('min_measure', s + 'min') \
            .withColumnRenamed('avg_measure', s + 'avg')
    ppd, pd, cd, pod = dfs

    joined_df = ppd\
        .join(pd, k, 'full') \
        .join(cd, k, 'full') \
        .join(pod, k, 'full')

    return joined_df


def sql_statement_02(four_windows_measure):
    joined_df = four_windows_measure
    # result_df = joined_df.select(
    #     col("person_id"),
    #     col("concept_name"),
    #     col("measurement_concept_id"),
    #     joined_df.pre_pre_max.alias('pre_pre_max'),
    #     joined_df.pre_pre_min.alias('pre_pre_min'),
    #     joined_df.pre_pre_avg.alias('pre_pre_avg'),
    #     joined_df.pre_max.alias('pre_max'),
    #     joined_df.pre_min.alias('pre_min'),
    #     joined_df.pre_avg.alias('pre_avg'),
    #     joined_df.covid_max.alias('covid_max'),
    #     joined_df.covid_min.alias('covid_min'),
    #     joined_df.covid_avg.alias('covid_avg'),
    #     joined_df.post_max.alias('post_max'),
    #     joined_df.post_min.alias('post_min'),
    #     joined_df.post_avg.alias('post_avg')
    # )

    return joined_df


def sql_statement_03(feature_table_builder, measurement_person):
    df = feature_table_builder.alias("feat").join(measurement_person.alias("m"),
                                                  (col("feat.person_id") == col("m.person_id")) & (
                                                      col("m.visit_date").between(col("feat.post_window_start_dt"),
                                                                                  col("feat.post_window_end_dt"))))

    result = df.groupBy("feat.person_id", "m.concept_name", "m.measurement_concept_id") \
        .agg(max("m.value_as_number").alias("max_measure"),
             min("m.value_as_number").alias("min_measure"),
             avg("m.value_as_number").alias("avg_measure"))
    return result


def sql_statement_04(feature_table_builder, measurement_person):
    df = feature_table_builder.alias("feat").join(measurement_person.alias("m"),
                                                  (col("feat.person_id") == col("m.person_id")) & (
                                                      col("m.visit_date").between(col("feat.pre_window_start_dt"),
                                                                                  col("feat.pre_window_end_dt"))))

    result = df.groupBy("feat.person_id", "m.concept_name", "m.measurement_concept_id") \
        .agg(max("m.value_as_number").alias("max_measure"),
             min("m.value_as_number").alias("min_measure"),
             avg("m.value_as_number").alias("avg_measure"))
    return result


def sql_statement_05(feature_table_builder, measurement_person):
    df = feature_table_builder.alias("feat").join(measurement_person.alias("m"),
                                                  (col("feat.person_id") == col("m.person_id")) & (
                                                      col("m.visit_date").between(col("feat.pre_pre_window_start_dt"),
                                                                                  col("feat.pre_window_start_dt"))))

    result = df.groupBy("feat.person_id", "m.concept_name", "m.measurement_concept_id") \
        .agg(max("m.value_as_number").alias("max_measure"),
             min("m.value_as_number").alias("min_measure"),
             avg("m.value_as_number").alias("avg_measure"))
    return result
