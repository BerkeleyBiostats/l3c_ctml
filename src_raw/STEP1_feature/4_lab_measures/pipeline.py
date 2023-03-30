from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType

import pandas as pd

def get_concept_id(concept, s):
    concepts_df = concept \
    .select('concept_set_name',
    'is_most_recent_version', 'concept_id') \
    .where(F.col('is_most_recent_version')=='true')

    ids = list(concepts_df.where(
    (concepts_df.concept_set_name==s) 
    & (concepts_df.is_most_recent_version=='true')
    ).select('concept_id').toPandas()['concept_id'])
    return ids

def get_dataframe(df, col_name, ids):
    # patient who do not have 
    sub_df = df.where(F.col('harmonized_value_as_number').isNotNull()) \
    .withColumn(col_name, F.when(df.measurement_concept_id.isin
    (ids), df.harmonized_value_as_number).otherwise(0))

    # aggregate by person and date
    sub_df = sub_df.groupby('person_id', 'visit_date').agg(
    F.max(col_name).alias(col_name))
    return sub_df

@transform_pandas(
    Output(rid="ri.vector.main.execute.b6af7e97-05b8-4768-9552-1e4046b18cf0"),
    Feature_table_builder=Input(rid="ri.foundry.main.dataset.ce7a93a0-4140-4fdb-b97d-fb78c0caf345"),
    measurement=Input(rid="ri.foundry.main.dataset.5c8b84fb-814b-4ee5-a89a-9525f4a617c7")
)
def measurement_person(measurement, Feature_table_builder):
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
     'Creatinine; blood'] # there are 32

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

