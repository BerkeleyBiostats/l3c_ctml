from pyspark.sql.functions import *
from pyspark.sql.functions import col


def sql_statement_00(concept):
    df = concept.filter(
        (concept.domain_id == "Drug") &
        (lower(concept.vocabulary_id) == "rxnorm") &
        (concept.concept_class_id == "Ingredient") &
        (concept.standard_concept == "S")
    ).select("*")
    return (df)


def sql_statement_01(Feature_table_builder, drug_exposure):
    d = drug_exposure.alias("d")
    f = Feature_table_builder.alias("f")

    result = d.join(f, col("d.person_id") == col("f.person_id"), "inner") \
        .select("d.*")
    return (result)


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
    return (result)


def sql_statement_03(Feature_table_builder, covid_drugs, microvisits_to_macrovisits):
    feat = Feature_table_builder.alias("feat")
    mml = microvisits_to_macrovisits.alias("mml")  # Alias microvisits_to_macrovisits dataset
    prc = covid_drugs.alias("prc")  # Alias covid_drugs dataset

    result = feat.join(mml, ["person_id"]) \
        .join(prc, ["visit_occurrence_id"]) \
        .groupBy("feat.person_id", "feat.patient_group", "prc.ancestor_drug_concept_name",
                 "prc.ancestor_drug_concept_id") \
        .agg(countDistinct(mml.visit_occurrence_id).alias("med_count"), lit("covid count").alias("count_type")) \
        .select("person_id", "patient_group", "ancestor_drug_concept_name", "ancestor_drug_concept_id", "count_type",
                "med_count")

    return (result)


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
    return (result)


def sql_statement_06(Feature_table_builder, post_drugs, microvisits_to_macrovisits):
    feat = Feature_table_builder.alias("feat")
    mml = microvisits_to_macrovisits.alias("mml")  # Alias microvisits_to_macrovisits dataset
    prc = post_drugs.alias("prc")  # Alias covid_drugs dataset

    result = feat.join(mml, ["person_id"]) \
        .join(prc, ["visit_occurrence_id"]) \
        .groupBy("feat.person_id", "feat.patient_group", "prc.ancestor_drug_concept_name",
                 "prc.ancestor_drug_concept_id") \
        .agg(countDistinct(mml.visit_occurrence_id).alias("med_count"), lit("covid count").alias("count_type")) \
        .select("person_id", "patient_group", "ancestor_drug_concept_name", "ancestor_drug_concept_id", "count_type",
                "med_count")

    return (result)


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
    return (result)


def sql_statement_08(covidtbl, posttbl, prepretbl, pretbl):
    prepretbl = prepretbl.alias("prepretbl")
    pretbl = pretbl.alias("pretbl")
    covidtbl = covidtbl.alias("covidtbl")
    posttbl = posttbl.alias("posttbl")

    result = (
        prepretbl.alias("prepretbl")
            .join(pretbl.alias("pretbl"), (col("pretbl.person_id") == col("prepretbl.person_id")) & (
                    col("pretbl.ancestor_drug_concept_name") == col("prepretbl.ancestor_drug_concept_name")),
                  how="full")
            .join(covidtbl.alias("covidtbl"), (col("covidtbl.person_id") == col("pretbl.person_id")) & (
                    col("covidtbl.ancestor_drug_concept_name") == col("pretbl.ancestor_drug_concept_name")), how="full")
            .join(posttbl.alias("posttbl"), (col("posttbl.person_id") == col("covidtbl.person_id")) & (
                    col("posttbl.ancestor_drug_concept_name") == col("covidtbl.ancestor_drug_concept_name")),
                  how="full")
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

    return (result)


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

    result = tbl.join(feat, "person_id") \
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
    return (result)


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
    return (result)


def sql_statement_11(Feature_table_builder, pre_pre_drugs, microvisits_to_macrovisits):
    feat = Feature_table_builder.alias("feat")
    mml = microvisits_to_macrovisits.alias("mml")  # Alias microvisits_to_macrovisits dataset
    prc = pre_pre_drugs.alias("prc")  # Alias covid_drugs dataset

    result = feat.join(mml, ["person_id"]) \
        .join(prc, ["visit_occurrence_id"]) \
        .groupBy("feat.person_id", "feat.patient_group", "prc.ancestor_drug_concept_name",
                 "prc.ancestor_drug_concept_id") \
        .agg(countDistinct(mml.visit_occurrence_id).alias("med_count"), lit("covid count").alias("count_type")) \
        .select("person_id", "patient_group", "ancestor_drug_concept_name", "ancestor_drug_concept_id", "count_type",
                "med_count")

    return (result)


def sql_statement_12(Feature_table_builder, pre_drugs, microvisits_to_macrovisits):
    feat = Feature_table_builder.alias("feat")
    mml = microvisits_to_macrovisits.alias("mml")  # Alias microvisits_to_macrovisits dataset
    prc = pre_drugs.alias("prc")  # Alias covid_drugs dataset

    result = feat.join(mml, ["person_id"]) \
        .join(prc, ["visit_occurrence_id"]) \
        .groupBy("feat.person_id", "feat.patient_group", "prc.ancestor_drug_concept_name",
                 "prc.ancestor_drug_concept_id") \
        .agg(countDistinct(mml.visit_occurrence_id).alias("med_count"), lit("covid count").alias("count_type")) \
        .select("person_id", "patient_group", "ancestor_drug_concept_name", "ancestor_drug_concept_id", "count_type",
                "med_count")

    return (result)
