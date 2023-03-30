

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.bf7621a4-7d12-4846-99bd-61b518242b14"),
    obs_person_clean=Input(rid="ri.vector.main.execute.6b72ca05-314e-44a8-bf4d-b16482206e54")
)
from pyspark.sql import functions as F
def obs_person_pivot(obs_person_clean):
    df = obs_person_clean
    df = df.withColumn("observation_concept_name", F.lower(F.regexp_replace(df["observation_concept_name"], "[^A-Za-z_0-9]", "_" )))
    df = df.groupby("person_id").pivot("observation_concept_name").agg(F.count('person_id').alias('_obs_ind'))
    
    df = df.fillna(0)

    return df
    

