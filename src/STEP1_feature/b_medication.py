from pyspark.sql.functions import *
from pyspark.sql.functions import col

##@transform_pandas(
##    Output(rid="ri.vector.main.execute.a63a8ea7-8537-4ef5-9e68-274c3b5bb545"),
##    concept=Input(rid="ri.foundry.main.dataset.5cb3c4a3-327a-47bf-a8bf-daf0cafe6772")
#)
def sql_statement_00(concept):
    df = concept.filter(
        (concept.domain_id == "Drug") &
        (lower(concept.vocabulary_id) == "rxnorm") &
        (concept.concept_class_id == "Ingredient") &
        (concept.standard_concept == "S")
    ).select("*")
    return(df)

##@transform_pandas(
##    Output(rid="ri.vector.main.execute.288cdbd1-0d61-4a65-a86d-1714402f663f"),
##    Feature_table_builder=Input(rid="ri.foundry.main.dataset.ce7a93a0-4140-4fdb-b97d-fb78c0caf345"),
##    drug_exposure=Input(rid="ri.foundry.main.dataset.469b3181-6336-4d0e-8c11-5e33a99876b5")
#)
def sql_statement_01(Feature_table_builder, drug_exposure):
    d = drug_exposure.alias("d")
    f = Feature_table_builder.alias("f")

    result = d.join(f, col("d.person_id") == col("f.person_id"), "inner") \
            .select("d.*")
    return(result)

##@transform_pandas(
##    Output(rid="ri.vector.main.execute.130284d0-8168-4ecb-8556-9646aa90cd07"),
##    Feature_table_builder=Input(rid="ri.foundry.main.dataset.ce7a93a0-4140-4fdb-b97d-fb78c0caf345"),
##    drugRollUp=Input(rid="ri.vector.main.execute.685e760b-462c-47c5-b2b2-5e8d9b1b4973")
#)
def sql_statement_02(Feature_table_builder, drugRollUp):
    feat = Feature_table_builder.alias("feat")
    co = drugRollUp.alias("co")

    result = feat.join(
        co,
        (feat.person_id == co.person_id) &
        (co.drug_exposure_start_date.between(feat.pre_window_end_dt, feat.post_window_start_dt))
    ).select(
        col("feat.*"),
        col("co.ancestor_drug_concept_name"),
        col("co.ancestor_drug_concept_id"),
        col("co.drug_exposure_start_date"),
        col("co.visit_occurrence_id")
    )
    return(result)

##@transform_pandas(
##    Output(rid="ri.vector.main.execute.99e86a2a-69e5-4c25-b6e8-6ea4ade87b3f"),
##    Feature_table_builder=Input(rid="ri.foundry.main.dataset.ce7a93a0-4140-4fdb-b97d-fb78c0caf345"),
##    covid_drugs=Input(rid="ri.vector.main.execute.130284d0-8168-4ecb-8556-9646aa90cd07"),
##    microvisits_to_macrovisits=Input(rid="ri.foundry.main.dataset.d77a701f-34df-48a1-a71c-b28112a07ffa")
#)
def sql_statement_03(Feature_table_builder, covid_drugs, microvisits_to_macrovisits):
    feat = Feature_table_builder.alias("feat")
    mml = microvisits_to_macrovisits.alias("mml") # Alias microvisits_to_macrovisits dataset
    prc = covid_drugs.alias("prc") # Alias covid_drugs dataset

    result = feat.join(mml, ["person_id"]) \
                .join(prc, ["visit_occurrence_id"]) \
                .groupBy("feat.person_id", "feat.patient_group", "prc.ancestor_drug_concept_name", "prc.ancestor_drug_concept_id") \
                .agg(countDistinct(mml.visit_occurrence_id).alias("med_count"), lit("covid count").alias("count_type")) \
                .select("person_id", "patient_group", "ancestor_drug_concept_name", "ancestor_drug_concept_id", "count_type", "med_count")

    return(result)

##@transform_pandas(
##    Output(rid="ri.vector.main.execute.685e760b-462c-47c5-b2b2-5e8d9b1b4973"),
##    DrugConcepts=Input(rid="ri.vector.main.execute.a63a8ea7-8537-4ef5-9e68-274c3b5bb545"),
##    Drugs_for_These_Patients=Input(rid="ri.vector.main.execute.288cdbd1-0d61-4a65-a86d-1714402f663f"),
##    concept_ancestor=Input(rid="ri.foundry.main.dataset.c5e0521a-147e-4608-b71e-8f53bcdbe03c")
#)
def sql_statement_04(DrugConcepts, Drugs_for_These_Patients):
    ds = Drugs_for_These_Patients.alias("ds")
    dc = DrugConcepts.alias("dc")

    result = ds.join(dc, col("dc.concept_id") == col("ds.drug_concept_id")) \
            .select(col("ds.person_id"), 
                    col("ds.drug_exposure_start_date"), 
                    col("ds.visit_occurrence_id"), 
                    col("dc.concept_id").alias("ancestor_drug_concept_id"), 
                    col("dc.concept_name").alias("ancestor_drug_concept_name")) \
            .distinct()
    return result

##@transform_pandas(
##    Output(rid="ri.vector.main.execute.d7cd5658-cbb1-418e-9b84-a8777aa67f19"),
##    Feature_table_builder=Input(rid="ri.foundry.main.dataset.ce7a93a0-4140-4fdb-b97d-fb78c0caf345"),
##    drugRollUp=Input(rid="ri.vector.main.execute.685e760b-462c-47c5-b2b2-5e8d9b1b4973")
#)
def sql_statement_05(Feature_table_builder, drugRollUp):
    feat = Feature_table_builder.alias("feat")
    co = drugRollUp.alias("co")

    result = feat.join(
        co,
        (feat.person_id == co.person_id) &
        (co.drug_exposure_start_date.between(feat.post_window_start_dt, feat.post_window_end_dt))
    ).select(
        col("feat.*"),
        col("co.ancestor_drug_concept_name"),
        col("co.ancestor_drug_concept_id"),
        col("co.drug_exposure_start_date"),
        col("co.visit_occurrence_id")
    )
    return(result)

##@transform_pandas(
##    Output(rid="ri.vector.main.execute.f7b478eb-85f4-43a0-949c-2ecc78140e17"),
##    Feature_table_builder=Input(rid="ri.foundry.main.dataset.ce7a93a0-4140-4fdb-b97d-fb78c0caf345"),
##    microvisits_to_macrovisits=Input(rid="ri.foundry.main.dataset.d77a701f-34df-48a1-a71c-b28112a07ffa"),
##    post_drugs=Input(rid="ri.vector.main.execute.d7cd5658-cbb1-418e-9b84-a8777aa67f19")
#)
def sql_statement_06(Feature_table_builder, post_drugs, microvisits_to_macrovisits):
    feat = Feature_table_builder.alias("feat")
    mml = microvisits_to_macrovisits.alias("mml") # Alias microvisits_to_macrovisits dataset
    prc = post_drugs.alias("prc") # Alias covid_drugs dataset

    result = feat.join(mml, ["person_id"]) \
                .join(prc, ["visit_occurrence_id"]) \
                .groupBy("feat.person_id", "feat.patient_group", "prc.ancestor_drug_concept_name", "prc.ancestor_drug_concept_id") \
                .agg(countDistinct(mml.visit_occurrence_id).alias("med_count"), lit("covid count").alias("count_type")) \
                .select("person_id", "patient_group", "ancestor_drug_concept_name", "ancestor_drug_concept_id", "count_type", "med_count")

    return(result)

##@transform_pandas(
##    Output(rid="ri.vector.main.execute.6420351f-3985-4ec5-b098-66c21eb6900a"),
##    Feature_table_builder=Input(rid="ri.foundry.main.dataset.ce7a93a0-4140-4fdb-b97d-fb78c0caf345"),
##    drugRollUp=Input(rid="ri.vector.main.execute.685e760b-462c-47c5-b2b2-5e8d9b1b4973")
#)
def sql_statement_07(Feature_table_builder, drugRollUp):
    feat = Feature_table_builder.alias("feat")
    co = drugRollUp.alias("co")

    result = feat.join(
        co,
        (feat.person_id == co.person_id) &
        (co.drug_exposure_start_date.between(feat.pre_window_start_dt, feat.pre_window_end_dt))
    ).select(
        col("feat.*"),
        col("co.ancestor_drug_concept_name"),
        col("co.ancestor_drug_concept_id"),
        col("co.drug_exposure_start_date"),
        col("co.visit_occurrence_id")
    )
    return(result)

##@transform_pandas(
##    Output(rid="ri.vector.main.execute.3bd5ba7c-d0c2-4485-9d10-c16895794ea0"),
##    covidtbl=Input(rid="ri.vector.main.execute.99e86a2a-69e5-4c25-b6e8-6ea4ade87b3f"),
##    posttbl=Input(rid="ri.vector.main.execute.f7b478eb-85f4-43a0-949c-2ecc78140e17"),
##    prepretbl=Input(rid="ri.vector.main.execute.753f92de-1931-408e-ade4-9d18a7f4bb76"),
##    pretbl=Input(rid="ri.vector.main.execute.0c161a09-22c1-421b-b6ef-df510fa5d02c")
#)
def sql_statement_08(covidtbl, posttbl, prepretbl, pretbl):
    prepretbl = prepretbl.alias("prepretbl")
    pretbl = pretbl.alias("pretbl")
    covidtbl = covidtbl.alias("covidtbl")
    posttbl = posttbl.alias("posttbl")

    result = (
        prepretbl.alias("prepretbl")
        .join(pretbl.alias("pretbl"), (col("pretbl.person_id") == col("prepretbl.person_id")) & (col("pretbl.ancestor_drug_concept_name") == col("prepretbl.ancestor_drug_concept_name")), how="full")
        .join(covidtbl.alias("covidtbl"), (col("covidtbl.person_id") == col("pretbl.person_id")) & (col("covidtbl.ancestor_drug_concept_name") == col("pretbl.ancestor_drug_concept_name")), how="full")
        .join(posttbl.alias("posttbl"), (col("posttbl.person_id") == col("covidtbl.person_id")) & (col("posttbl.ancestor_drug_concept_name") == col("covidtbl.ancestor_drug_concept_name")), how="full")
        .select(
            col("prepretbl.person_id").alias("prepre_person_id"),
            col("prepretbl.patient_group").alias("prepre_patient_group"),
            col("prepretbl.ancestor_drug_concept_name").alias("prepre_ancestor_drug_concept_name"),
            col("prepretbl.ancestor_drug_concept_id").alias("prepre_ancestor_drug_concept_id"),
            col("prepretbl.count_type").alias("prepre_count_type"),
            col("prepretbl.med_count").alias("prepre_med_count"),
            col("pretbl.person_id").alias("pre_person_id"),
            col("pretbl.patient_group").alias("pre_patient_group"),
            col("pretbl.ancestor_drug_concept_name").alias("pre_ancestor_drug_concept_name"),
            col("pretbl.ancestor_drug_concept_id").alias("pre_ancestor_drug_concept_id"),
            col("pretbl.count_type").alias("pre_count_type"),
            col("pretbl.med_count").alias("pre_med_count"),
            col("covidtbl.person_id").alias("covid_person_id"),
            col("covidtbl.patient_group").alias("covid_patient_group"),
            col("covidtbl.ancestor_drug_concept_name").alias("covid_ancestor_drug_concept_name"),
            col("covidtbl.ancestor_drug_concept_id").alias("covid_ancestor_drug_concept_id"),
            col("covidtbl.count_type").alias("covid_count_type"),
            col("covidtbl.med_count").alias("covid_med_count"),
            col("posttbl.person_id").alias("post_person_id"),
            col("posttbl.patient_group").alias("post_patient_group"),
            col("posttbl.ancestor_drug_concept_name").alias("post_ancestor_drug_concept_name"),
            col("posttbl.ancestor_drug_concept_id").alias("post_ancestor_drug_concept_id"),
            col("posttbl.count_type").alias("post_count_type"),
            col("posttbl.med_count").alias("post_med_count")
        )
    )
    
    return(result)

##@transform_pandas(
##    Output(rid="ri.foundry.main.dataset.fa3fe17e-58ac-4615-a238-0fc24ecd9b6e"),
##    Feature_table_builder=Input(rid="ri.foundry.main.dataset.ce7a93a0-4140-4fdb-b97d-fb78c0caf345"),
##    pre_post_med_count=Input(rid="ri.vector.main.execute.3bd5ba7c-d0c2-4485-9d10-c16895794ea0")
#)
def sql_statement_09(Feature_table_builder, pre_post_med_count):
    feat = Feature_table_builder
    pp = pre_post_med_count

    tbl = pre_post_med_count.select(
            coalesce(pp["prepre_person_id"], 
                     pp["pre_person_id"], 
                     pp["covid_person_id"], 
                     pp["post_person_id"]).alias("person_id"),
            coalesce(pp["prepre_patient_group"], 
                     pp["pre_patient_group"], 
                     pp["covid_patient_group"], 
                     pp["post_patient_group"]).alias("patient_group"),
            coalesce(pp["prepre_ancestor_drug_concept_name"], 
                     pp["pre_ancestor_drug_concept_name"], 
                     pp["covid_ancestor_drug_concept_name"], 
                     pp["post_ancestor_drug_concept_name"]).alias("ancestor_drug_concept_name"),
            coalesce(pp["prepre_ancestor_drug_concept_id"], 
                     pp["pre_ancestor_drug_concept_id"], 
                     pp["covid_ancestor_drug_concept_id"], 
                     pp["post_ancestor_drug_concept_id"]).alias("ancestor_drug_concept_id"),
            coalesce(pp["prepre_med_count"], lit(0)).alias("pre_pre_med_count"),
            coalesce(pp["pre_med_count"], lit(0)).alias("pre_med_count"),
            coalesce(pp["covid_med_count"], lit(0)).alias("covid_med_count"),
            coalesce(pp["post_med_count"], lit(0)).alias("post_med_count")
    )

    result = tbl.join(feat, "person_id")\
        .select(
            tbl["*"],
            col("apprx_age"),
            col("sex"),
            col("race"),
            col("ethn"),
            col("tot_long_data_days"),
            col("op_post_visit_ratio"),
            col("post_ip_visit_ratio"),
            col("covid_ip_visit_ratio"),
            col("post_icu_visit_ratio"),
            col("covid_icu_visit_ratio")
        ).distinct()
    return(result)


##@transform_pandas(
##    Output(rid="ri.vector.main.execute.9b9a05ee-943a-4764-9585-7f94c813af83"),
##    Feature_table_builder=Input(rid="ri.foundry.main.dataset.ce7a93a0-4140-4fdb-b97d-fb78c0caf345"),
##    drugRollUp=Input(rid="ri.vector.main.execute.685e760b-462c-47c5-b2b2-5e8d9b1b4973")
#)
def sql_statement_10(Feature_table_builder, drugRollUp):
    feat = Feature_table_builder.alias("feat")
    co = drugRollUp.alias("co")

    result = feat.join(
        co,
        (feat.person_id == co.person_id) &
        (co.drug_exposure_start_date.between(feat.pre_pre_window_start_dt, feat.pre_window_start_dt))
    ).select(
        col("feat.*"),
        col("co.ancestor_drug_concept_name"),
        col("co.ancestor_drug_concept_id"),
        col("co.drug_exposure_start_date"),
        col("co.visit_occurrence_id")
    )
    return(result)

##@transform_pandas(
##    Output(rid="ri.vector.main.execute.753f92de-1931-408e-ade4-9d18a7f4bb76"),
##    Feature_table_builder=Input(rid="ri.foundry.main.dataset.ce7a93a0-4140-4fdb-b97d-fb78c0caf345"),
##    microvisits_to_macrovisits=Input(rid="ri.foundry.main.dataset.d77a701f-34df-48a1-a71c-b28112a07ffa"),
##    pre_pre_drugs=Input(rid="ri.vector.main.execute.9b9a05ee-943a-4764-9585-7f94c813af83")
#)
def sql_statement_11(Feature_table_builder, pre_pre_drugs, microvisits_to_macrovisits):
    feat = Feature_table_builder.alias("feat")
    mml = microvisits_to_macrovisits.alias("mml") # Alias microvisits_to_macrovisits dataset
    prc = pre_pre_drugs.alias("prc") # Alias covid_drugs dataset

    result = feat.join(mml, ["person_id"]) \
                .join(prc, ["visit_occurrence_id"]) \
                .groupBy("feat.person_id", "feat.patient_group", "prc.ancestor_drug_concept_name", "prc.ancestor_drug_concept_id") \
                .agg(countDistinct(mml.visit_occurrence_id).alias("med_count"), lit("covid count").alias("count_type")) \
                .select("person_id", "patient_group", "ancestor_drug_concept_name", "ancestor_drug_concept_id", "count_type", "med_count")

    return(result)

##@transform_pandas(
##    Output(rid="ri.vector.main.execute.0c161a09-22c1-421b-b6ef-df510fa5d02c"),
##    Feature_table_builder=Input(rid="ri.foundry.main.dataset.ce7a93a0-4140-4fdb-b97d-fb78c0caf345"),
##    microvisits_to_macrovisits=Input(rid="ri.foundry.main.dataset.d77a701f-34df-48a1-a71c-b28112a07ffa"),
##    pre_drugs=Input(rid="ri.vector.main.execute.6420351f-3985-4ec5-b098-66c21eb6900a")
#)
def sql_statement_12(Feature_table_builder, pre_drugs, microvisits_to_macrovisits):
    feat = Feature_table_builder.alias("feat")
    mml = microvisits_to_macrovisits.alias("mml") # Alias microvisits_to_macrovisits dataset
    prc = pre_drugs.alias("prc") # Alias covid_drugs dataset

    result = feat.join(mml, ["person_id"]) \
                .join(prc, ["visit_occurrence_id"]) \
                .groupBy("feat.person_id", "feat.patient_group", "prc.ancestor_drug_concept_name", "prc.ancestor_drug_concept_id") \
                .agg(countDistinct(mml.visit_occurrence_id).alias("med_count"), lit("covid count").alias("count_type")) \
                .select("person_id", "patient_group", "ancestor_drug_concept_name", "ancestor_drug_concept_id", "count_type", "med_count")

    return(result)

