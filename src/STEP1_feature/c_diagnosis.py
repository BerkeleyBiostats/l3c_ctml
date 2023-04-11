
from pyspark.sql.functions import *

def sql_statement_00(Feature_table_builder, condition_occurrence):
    feat = Feature_table_builder
    co = condition_occurrence

    joined_df = feat.join(
        co,
        (feat.person_id == co.person_id) & (co.condition_start_date.between(feat.pre_window_end_dt, feat.post_window_start_dt))
    ).select(
        feat["*"],
        # co.condition_concept_name,
        co.condition_concept_id,
        co.condition_start_date,
        co.condition_source_value,
        co.visit_occurrence_id
    )
    return(joined_df)



def sql_statement_01(Feature_Table_Builder, microvisits_to_macrovisits, pre_pre_condition, pre_condition, covid_condition, post_condition):
    feat = Feature_Table_Builder
    mml = microvisits_to_macrovisits

    prepretbl = (feat.join(mml, feat.person_id == mml.person_id)
                 .join(pre_pre_condition, mml.visit_occurrence_id == pre_pre_condition.visit_occurrence_id)
                 .groupBy(feat.person_id, feat.patient_group, pre_pre_condition.condition_concept_id)
                 .agg(countDistinct(when(mml.visit_occurrence_id.isNotNull(), mml.visit_occurrence_id)))
                 .withColumnRenamed('count(CASE WHEN (visit_occurrence_id IS NOT NULL) THEN visit_occurrence_id END)', 'dx_count')
                 .withColumn('count_type', when(col('dx_count').isNull(), None).otherwise('pre count'))
                 .withColumnRenamed("dx_count", "pre_pre_dx_count")
                   )

    pretbl = (feat.join(mml, feat.person_id == mml.person_id)
              .join(pre_condition, mml.visit_occurrence_id == pre_condition.visit_occurrence_id)
              .groupBy(feat.person_id, feat.patient_group, pre_condition.condition_concept_id)
              .agg(countDistinct(when(mml.visit_occurrence_id.isNotNull(), mml.visit_occurrence_id)))
              .withColumnRenamed('count(CASE WHEN (visit_occurrence_id IS NOT NULL) THEN visit_occurrence_id END)', 'dx_count')
              .withColumn('count_type', when(col('dx_count').isNull(), None).otherwise('pre count'))
              .withColumnRenamed("dx_count", "pre_dx_count")
                   )

    covidtbl = (feat.join(mml, feat.person_id == mml.person_id)
                .join(covid_condition, mml.visit_occurrence_id == covid_condition.visit_occurrence_id)
                .groupBy(feat.person_id, feat.patient_group, covid_condition.condition_concept_id)
                .agg(countDistinct(when(mml.visit_occurrence_id.isNotNull(), mml.visit_occurrence_id)))
                .withColumnRenamed('count(CASE WHEN (visit_occurrence_id IS NOT NULL) THEN visit_occurrence_id END)', 'dx_count')
                .withColumn('count_type', when(col('dx_count').isNull(), None).otherwise('pre count'))
                .withColumnRenamed("dx_count", "covid_dx_count")
                   )

    posttbl = (feat.join(mml, feat.person_id == mml.person_id)
               .join(post_condition, mml.visit_occurrence_id == post_condition.visit_occurrence_id)
               .groupBy(feat.person_id, feat.patient_group, post_condition.condition_concept_id)
               .agg(countDistinct(when(mml.visit_occurrence_id.isNotNull(), mml.visit_occurrence_id)))
               .withColumnRenamed('count(CASE WHEN (visit_occurrence_id IS NOT NULL) THEN visit_occurrence_id END)', 'dx_count')
               .withColumn('count_type', when(col('dx_count').isNull(), None).otherwise('pre count'))
               .withColumnRenamed("dx_count", "post_dx_count")
                   )

    full_join1 = prepretbl.join(pretbl, on=["person_id", "patient_group", "condition_concept_id"], how="full")
    full_join2 = full_join1.join(covidtbl, on=["person_id", "patient_group", "condition_concept_id"], how="full")
    full_join3 = full_join2.join(posttbl, on=["person_id", "patient_group", "condition_concept_id"], how="full")
    
    return(full_join3)
    
    

def sql_statement_02(Feature_table_builder, condition_occurrence):
    feat = Feature_table_builder
    co = condition_occurrence

    joined_df = feat.join(
        co,
        (feat.person_id == co.person_id) & (co.condition_start_date.between(feat.post_window_start_dt, feat.post_window_end_dt))
    ).select(
        feat["*"],
        # co.condition_concept_name,
        co.condition_concept_id,
        co.condition_start_date,
        co.condition_source_value,
        co.visit_occurrence_id
    )
    return(joined_df)



def sql_statement_03(Feature_table_builder, condition_occurrence):
    feat = Feature_table_builder
    co = condition_occurrence

    joined_df = feat.join(
        co,
        (feat.person_id == co.person_id) & (co.condition_start_date.between(feat.pre_window_start_dt, feat.pre_window_end_dt))
    ).select(
        feat["*"],
        # co.condition_concept_name,
        co.condition_concept_id,
        co.condition_start_date,
        co.condition_source_value,
        co.visit_occurrence_id
    )
    return(joined_df)


def sql_statement_04(Feature_Table_Builder, four_windows_dx_counts):

    feat = Feature_Table_Builder


    tbl = four_windows_dx_counts.select(
        col("person_id"), col("patient_group"), col("condition_concept_id"), 
        coalesce(col("pre_pre_dx_count"), col("pre_dx_count"), col("covid_dx_count"), col("post_dx_count"), lit(0)).alias("pre_pre_dx_count"),
        coalesce(col("pre_dx_count"), lit(0)).alias("pre_dx_count"),
        coalesce(col("covid_dx_count"), lit(0)).alias("covid_dx_count"),
        coalesce(col("post_dx_count"), lit(0)).alias("post_dx_count")
    )

    result = feat.join(
        tbl,
        on="person_id",
        how="inner"
    ).select(
        feat["*"],
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



def sql_statement_05(Feature_table_builder, condition_occurrence):
    feat = Feature_table_builder
    co = condition_occurrence

    joined_df = feat.join(
        co,
        (feat.person_id == co.person_id) & (co.condition_start_date.between(feat.pre_pre_window_start_dt, feat.pre_window_start_dt))
    ).select(
        feat["*"],
        # co.condition_concept_name,
        co.condition_concept_id,
        co.condition_start_date,
        co.condition_source_value,
        co.visit_occurrence_id
    )
    return(joined_df)
