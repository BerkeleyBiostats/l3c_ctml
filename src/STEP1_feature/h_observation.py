##@transform_pandas(
##    Output(rid="ri.vector.main.execute.bb5738c9-628e-4466-a2ce-cde2c411aef1"),
##    Feature_table_builder=Input(rid="ri.foundry.main.dataset.ce7a93a0-4140-4fdb-b97d-fb78c0caf345"),
##    observation=Input(rid="ri.foundry.main.dataset.f9d8b08e-3c9f-4292-b603-f1bfa4336516")
# )
from pyspark.sql.functions import *


def sql_statement_00(Feature_table_builder, observation, concept):
    feat = Feature_table_builder
    obs_concept_names = ['Never smoker', 'Former smoker', 'Current every day smoker',
                         'Long-term current use of insulin', 'Long-term current use of anticoagulant',
                         'Denies alcohol use', 'Admits alcohol use', 'Body mass index 30+ - obesity',
                         'Body mass index 40+ - severely obese', 'Marital status [NHANES]',
                         'Dependence on renal dialysis']

    obs = observation.join(feat, "person_id", 'left') \
        .join(concept, (observation.observation_concept_id == concept.concept_id), 'left') \
        .filter(col("concept_name").isin(obs_concept_names)) \
        .groupBy("person_id", "concept_name") \
        .agg(max("concept_name").alias("observation_concept_name")).\
        select("person_id", "observation_concept_name")
    return obs



##@transform_pandas(
##    Output(rid="ri.vector.main.execute.6b72ca05-314e-44a8-bf4d-b16482206e54"),
##    obs_person=Input(rid="ri.vector.main.execute.bb5738c9-628e-4466-a2ce-cde2c411aef1")
# )
# check if this kind of observation appears more than 10 time
def sql_statement_01(obs_person):
    # l = list(obs_person.groupBy("observation_concept_name").count().filter(col("count") > 10).select(
    #     "observation_concept_name").toPandas())
    # df = obs_person.filter(col("observation_concept_name").isin(l))
    df = obs_person
    return df


def obs_person_pivot(obs_person_clean):
    df = obs_person_clean
    df = df.withColumn("observation_concept_name",
                       lower(regexp_replace(df["observation_concept_name"], "[^A-Za-z_0-9]", "_")))
    df = df.groupby("person_id").pivot("observation_concept_name").agg(count('person_id').alias('_obs_ind'))

    df = df.fillna(0)
    return df
