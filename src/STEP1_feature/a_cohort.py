#### @transform_pandas(
####     Output(rid="ri.vector.main.execute.b394ad88-ebb0-4f13-bf86-cfa6a7f5e612"),
####     Covid_Pasc_Index_Dates=Input(rid="ri.vector.main.execute.354cc0eb-336b-4864-b750-9d75bf0a8ba4"),
####     manifest_safe_harbor=Input(rid="ri.foundry.main.dataset.b4407989-1851-4e07-a13f-0539fae10f26"),
####     person=Input(rid="ri.foundry.main.dataset.f71ffe18-6969-4a24-b81c-0e06a1ae9316")
# )
from pyspark.sql.functions import *


def sql_statement_00(covid_pasc_index_dates, person):
    # Join Covid_Pasc_Index_Dates and person tables
    df = covid_pasc_index_dates.join(person, "person_id")

    # Calculate age based on year of birth and run date from manifest_safe_harbor table
    df = df.withColumn("apprx_age", year(lit("2023-01-01")) - col("year_of_birth"))

    # Select distinct columns
    df = df.select(
        col("person_id"),
        col("apprx_age"),
        col("gender_source_value").alias("sex"),
        col("race_source_value").alias("race"),
        col("ethnicity_source_value").alias("ethn"),
        col("covid_index").alias("min_covid_dt")
    )

    # Filter based on the amount of post-covid data available
    # df = df.filter(datediff(lit("2023-01-01"), array_min(array(col("covid_index"), col("death_date")))) >= 100)
    return df


#
#### @transform_pandas(
####     Output(rid="ri.vector.main.execute.354cc0eb-336b-4864-b750-9d75bf0a8ba4"),
####     Long_COVID_Silver_Standard=Input(rid="ri.foundry.main.dataset.3ea1038c-e278-4b0e-8300-db37d3505671")
# )
def sql_statement_01(long_covid_silver_standard):
    long_covid_silver_standard = long_covid_silver_standard.withColumn("time_to_pasc", col("time_to_pasc").cast("int")) \
        .fillna({"time_to_pasc": 0}) \
        .withColumn("pasc_index", expr("date_add(covid_index, time_to_pasc)")) \
        .filter(col("pasc_code_prior_four_weeks") != 1)
    return long_covid_silver_standard


#### @transform_pandas(
####     Output(rid="ri.foundry.main.dataset.ce7a93a0-4140-4fdb-b97d-fb78c0caf345"),
####     Feature_Table_Builder_v0=Input(rid="ri.vector.main.execute.e26f3947-ea85-4de9-b662-4048a52ec048"),
####     tot_icu_days_calc=Input(rid="ri.vector.main.execute.e8f9f7e0-1c42-44d6-8fcd-20cc54971623"),
####     tot_ip_days_calc=Input(rid="ri.vector.main.execute.fe1ce00c-f84c-4fc6-b1bb-d3a268301ade")
# )
def sql_statement_02(Feature_Table_Builder_v0, tot_icu_days_calc, tot_ip_days_calc):
    feat = Feature_Table_Builder_v0
    tot_ip = tot_ip_days_calc
    tot_icu = tot_icu_days_calc
    feat = feat.join(tot_ip, ['person_id'], how='left') \
        .join(tot_icu, ['person_id'], how='left') \
        .select(feat['*'],
                (coalesce(tot_ip['post_tot_ip_days'], lit(0)) / feat['tot_post_days']).alias('post_ip_visit_ratio'),
                (coalesce(tot_ip['covid_tot_ip_days'], lit(0)) / feat['tot_covid_days']).alias('covid_ip_visit_ratio'),
                (coalesce(tot_icu['post_tot_icu_days'], lit(0)) / feat['tot_post_days']).alias('post_icu_visit_ratio'),
                (coalesce(tot_icu['covid_tot_icu_days'], lit(0)) / feat['tot_covid_days']).alias('covid_icu_visit_ratio'))
    return feat


#### @transform_pandas(
####     Output(rid="ri.vector.main.execute.e26f3947-ea85-4de9-b662-4048a52ec048"),
####     Covid_Pasc_Index_Dates=Input(rid="ri.vector.main.execute.354cc0eb-336b-4864-b750-9d75bf0a8ba4"),
####     hosp_and_non=Input(rid="ri.vector.main.execute.a20c0955-295e-48b1-9286-81621279712f"),
####     manifest_safe_harbor=Input(rid="ri.foundry.main.dataset.b4407989-1851-4e07-a13f-0539fae10f26"),
####     microvisits_to_macrovisits=Input(rid="ri.foundry.main.dataset.d77a701f-34df-48a1-a71c-b28112a07ffa")
# )
def sql_statement_03(Covid_Pasc_Index_Dates, hosp_and_non, microvisits_to_macrovisits):
    # calculate pre-covid, post-covid windows
    hc = hosp_and_non
    mm = microvisits_to_macrovisits
    lc = Covid_Pasc_Index_Dates.drop("ctc.min_covid_dt")

    ottbl = hc.alias('hc').join(lc, "person_id").join(mm, 'person_id').groupBy(
        hc.person_id, hc.patient_group, hc.apprx_age, hc.sex, hc.race, hc.ethn, hc.min_covid_dt, lc.pasc_index
    ).agg(
        min("hc.min_covid_dt").alias("min_covid_dt_2"),
        max("visit_start_date").alias("max_visit_start_date"),
        countDistinct("visit_start_date").alias("tot_long_data_days")
    ).drop("min_covid_dt") \
        .withColumn(
        "pre_pre_window_start_dt", date_add(col("min_covid_dt_2"), -365)
    ).withColumn(
        "pre_window_start_dt", date_add(col("min_covid_dt_2"), -37)
    ).withColumn(
        "pre_window_end_dt", date_add(col("min_covid_dt_2"), -7)
    ).withColumn(
        "post_window_start_dt", date_add(col("min_covid_dt_2"), 14)
    ).withColumn(
        "post_window_end_dt", date_add(col("min_covid_dt_2"), 28)
    )

    result_df = (
        ottbl.alias("o").join(mm, (
                (ottbl.person_id == mm.person_id) &
                (mm.visit_start_date.between(ottbl.post_window_start_dt, ottbl.post_window_end_dt)) &
                (mm.visit_occurrence_id.isNull())
        ), how="left").groupBy(
            "o.person_id", "patient_group", "apprx_age", "sex", "race", "ethn",
            "min_covid_dt_2", "tot_long_data_days", "pre_pre_window_start_dt", "pre_window_start_dt",
            "pre_window_end_dt", "post_window_start_dt", "post_window_end_dt"
        ).agg(
            countDistinct("visit_start_date").alias("post_visits_count"),
            datediff(col("post_window_end_dt"), col("post_window_start_dt")).alias("tot_post_days"),
            datediff(col("post_window_start_dt"), col("pre_window_end_dt")).alias("tot_covid_days"),
            (countDistinct("visit_start_date") / 14).alias("op_post_visit_ratio")
        )
    )

    return result_df


#### @transform_pandas(
####     Output(rid="ri.vector.main.execute.fafe2849-680c-4e7c-bd60-bc474da15887"),
####     Collect_the_Cohort=Input(rid="ri.vector.main.execute.b394ad88-ebb0-4f13-bf86-cfa6a7f5e612"),
####     condition_occurrence=Input(rid="ri.foundry.main.dataset.2f496793-6a4e-4bf4-b0fc-596b277fb7e2"),
####     microvisits_to_macrovisits=Input(rid="ri.foundry.main.dataset.d77a701f-34df-48a1-a71c-b28112a07ffa")
# )
def sql_statement_04(cohort, cond_occ, micro_to_macro):
    cohort = cohort.join(micro_to_macro, "person_id", "left") \
        .filter((micro_to_macro.visit_start_date >= date_add(cohort.min_covid_dt, -14)) &
                (micro_to_macro.visit_start_date <= date_add(cohort.min_covid_dt, 14))) \
        .select("person_id", "apprx_age", "sex", "race", "ethn", "min_covid_dt") \
        .union(
        cohort.alias('c').join(micro_to_macro, "person_id", "left")
            .join(cond_occ, cond_occ.visit_occurrence_id == micro_to_macro.visit_occurrence_id, "inner")
            .filter(cond_occ.condition_concept_id == 37311061)
            .select("c.person_id", "apprx_age", "sex", "race", "ethn", "min_covid_dt")
    )
    return cohort


#### @transform_pandas(
####     Output(rid="ri.vector.main.execute.e86d4e39-4ce0-4b57-b3ec-921a86640b88"),
####     microvisits_to_macrovisits=Input(rid="ri.foundry.main.dataset.d77a701f-34df-48a1-a71c-b28112a07ffa")
# )
def sql_statement_05(microvisits_to_macrovisits, concept):
    df = microvisits_to_macrovisits \
        .join(concept, (microvisits_to_macrovisits.visit_concept_id == concept.concept_id), 'left') \
        .select('person_id', 'concept_name', 'visit_occurrence_id', 'visit_start_date', 'visit_end_date') \
        .where(col('concept_name').isin('Inpatient Critical Care Facility',
                                        'Emergency Room and Inpatient Visit',
                                        'Emergency Room Visit',
                                        'Intensive Care',
                                        'Emergency Room - Hospital')) \
        .where(col('visit_occurrence_id').isNotNull())
    return (df)


#### @transform_pandas(
####     Output(rid="ri.vector.main.execute.a20c0955-295e-48b1-9286-81621279712f"),
####     Collect_the_Cohort=Input(rid="ri.vector.main.execute.b394ad88-ebb0-4f13-bf86-cfa6a7f5e612"),
####     Hospitalized_Cases=Input(rid="ri.vector.main.execute.fafe2849-680c-4e7c-bd60-bc474da15887")
# )
def sql_statement_06(cohort, hosp_cases):
    cohort = cohort.alias("ctc").join(hosp_cases.alias('hc'), "person_id", "left") \
        .select(col("ctc.person_id"),
                col("ctc.apprx_age"),
                col("ctc.sex"),
                col("ctc.race"),
                col("ctc.ethn"),
                col("ctc.min_covid_dt"),
                when(col("hc.apprx_age").isNull(), "CASE_NONHOSP")
                .otherwise("CASE_HOSP").alias("patient_group"))

    return cohort


#### @transform_pandas(
####     Output(rid="ri.vector.main.execute.3853f0d6-ac95-4675-bbd2-5a33395676ef"),
####     microvisits_to_macrovisits=Input(rid="ri.foundry.main.dataset.d77a701f-34df-48a1-a71c-b28112a07ffa")
# )
def sql_statement_07(microvisits_to_macrovisits, concept):
    df = microvisits_to_macrovisits \
        .join(concept, (microvisits_to_macrovisits.visit_concept_id == concept.concept_id), 'left') \
        .select('person_id', 'concept_name', 'visit_occurrence_id', 'visit_start_date', 'visit_end_date') \
        .where(col('concept_name').isin("Inpatient Visit", "Inpatient Hospital", "Inpatient Critical Care Facility",
                                        "Emergency Room and Inpatient Visit", "Emergency Room Visit", "Intensive Care",
                                        "Emergency Room - Hospital")) \
        .where(col('visit_occurrence_id').isNotNull())

    return df


#### @transform_pandas(
####     Output(rid="ri.foundry.main.dataset.34a5ed27-4c8c-49ae-b084-73bd73c79a49"),
####     Covid_Pasc_Index_Dates=Input(rid="ri.vector.main.execute.354cc0eb-336b-4864-b750-9d75bf0a8ba4")
# )
def sql_statement_08(covid_pasc_index_dates):
    long_covid_df = covid_pasc_index_dates.select(col("person_id"),
                                                  col("pasc_code_after_four_weeks").alias("long_covid"),
                                                  col("pasc_index")) \
        .filter(col("pasc_code_prior_four_weeks") != 1)
    return long_covid_df


#### @transform_pandas(
####     Output(rid="ri.vector.main.execute.e8f9f7e0-1c42-44d6-8fcd-20cc54971623"),
####     Feature_Table_Builder_v0=Input(rid="ri.vector.main.execute.e26f3947-ea85-4de9-b662-4048a52ec048"),
####     ICU_visits=Input(rid="ri.vector.main.execute.e86d4e39-4ce0-4b57-b3ec-921a86640b88")
# )
def sql_statement_09(Feature_Table_Builder_v0, icu_visits):
    feat = Feature_Table_Builder_v0

    tbl = feat.join(icu_visits, (feat.person_id == icu_visits.person_id) & \
                    (icu_visits.visit_start_date.between(feat.post_window_start_dt, feat.post_window_end_dt)),
                    "inner") \
        .select(feat.person_id, feat.post_window_start_dt, feat.post_window_end_dt, \
                icu_visits.visit_start_date, icu_visits.visit_end_date)

    post_tbl = tbl.groupBy("person_id", "post_window_start_dt", "post_window_end_dt") \
        .agg(max(col("visit_end_date")).alias("macrovisit_end_date"),
             min(col("visit_start_date")).alias("macrovisit_start_date")) \
        .select("*", when(datediff(col("macrovisit_end_date"), col("macrovisit_start_date")) >
                          datediff(col("post_window_end_dt"), col("macrovisit_start_date")),
                          datediff(col("post_window_end_dt"), col("macrovisit_start_date")))
                .otherwise(datediff(col("macrovisit_end_date"), col("macrovisit_start_date")))
                .alias("post_tot_icu_days"))

    tbl = feat.join(icu_visits, (feat.person_id == icu_visits.person_id) & \
                    (icu_visits.visit_start_date.between(feat.pre_window_end_dt, feat.post_window_start_dt)),
                    "inner") \
        .select(feat.person_id, feat.pre_window_end_dt, feat.post_window_start_dt, \
                icu_visits.visit_start_date, icu_visits.visit_end_date)

    covid_tbl = tbl.groupBy("person_id", "pre_window_end_dt", "post_window_start_dt") \
        .agg(max(col("visit_end_date")).alias("macrovisit_end_date"), \
             min(col("visit_start_date")).alias("macrovisit_start_date")) \
        .select("*", when(datediff(col("macrovisit_end_date"), col("macrovisit_start_date")) > \
                          datediff(col("post_window_start_dt"), col("macrovisit_start_date")), \
                          datediff(col("post_window_start_dt"), col("macrovisit_start_date"))) \
                .otherwise(datediff(col("macrovisit_end_date"), col("macrovisit_start_date"))) \
                .alias("covid_tot_icu_days"))

    post_tbl = post_tbl.withColumnRenamed("person_id", "person_id_post") \
        .withColumnRenamed("post_window_start_dt", "post_window_start_dt_post")

    joined_df = post_tbl.join(covid_tbl, (post_tbl.person_id_post == covid_tbl.person_id) & ((post_tbl.post_window_start_dt_post == covid_tbl.post_window_start_dt)),
                              how="full_outer") \
        .select(
        coalesce(post_tbl.person_id_post, covid_tbl.person_id).alias("person_id"),
        coalesce(post_tbl.post_window_start_dt_post, covid_tbl.post_window_start_dt).alias("post_window_start_dt"),
        post_tbl.post_window_end_dt,
        post_tbl.post_tot_icu_days,
        covid_tbl.pre_window_end_dt,
        covid_tbl.covid_tot_icu_days
    )
    return joined_df


#### @transform_pandas(
####     Output(rid="ri.vector.main.execute.fe1ce00c-f84c-4fc6-b1bb-d3a268301ade"),
####     Feature_Table_Builder_v0=Input(rid="ri.vector.main.execute.e26f3947-ea85-4de9-b662-4048a52ec048"),
####     inpatient_visits=Input(rid="ri.vector.main.execute.3853f0d6-ac95-4675-bbd2-5a33395676ef")
# )
def sql_statement_10(Feature_Table_Builder_v0, inpatient_visits):
    feat = Feature_Table_Builder_v0

    tbl = feat.join(inpatient_visits, (feat.person_id == inpatient_visits.person_id) & \
                    (inpatient_visits.visit_start_date.between(feat.post_window_start_dt, feat.post_window_end_dt)),
                    "inner") \
        .select(feat.person_id, feat.post_window_start_dt, feat.post_window_end_dt, \
                inpatient_visits.visit_start_date, inpatient_visits.visit_end_date)

    post_tbl = tbl.groupBy("person_id", "post_window_start_dt", "post_window_end_dt") \
        .agg(max(col("visit_end_date")).alias("macrovisit_end_date"), \
             min(col("visit_start_date")).alias("macrovisit_start_date")) \
        .select("*", when(datediff(col("macrovisit_end_date"), col("macrovisit_start_date")) > \
                          datediff(col("post_window_end_dt"), col("macrovisit_start_date")), \
                          datediff(col("post_window_end_dt"), col("macrovisit_start_date"))) \
                .otherwise(datediff(col("macrovisit_end_date"), col("macrovisit_start_date"))) \
                .alias("post_tot_ip_days"))

    tbl = feat.join(inpatient_visits, (feat.person_id == inpatient_visits.person_id) & \
                    (inpatient_visits.visit_start_date.between(feat.pre_window_end_dt, feat.post_window_start_dt)),
                    "inner") \
        .select(feat.person_id, feat.pre_window_end_dt, feat.post_window_start_dt, \
                inpatient_visits.visit_start_date, inpatient_visits.visit_end_date)

    covid_tbl = tbl.groupBy("person_id", "pre_window_end_dt", "post_window_start_dt") \
        .agg(max(col("visit_end_date")).alias("macrovisit_end_date"), \
             min(col("visit_start_date")).alias("macrovisit_start_date")) \
        .select("*", when(datediff(col("macrovisit_end_date"), col("macrovisit_start_date")) > \
                          datediff(col("post_window_start_dt"), col("macrovisit_start_date")), \
                          datediff(col("post_window_start_dt"), col("macrovisit_start_date"))) \
                .otherwise(datediff(col("macrovisit_end_date"), col("macrovisit_start_date"))) \
                .alias("covid_tot_ip_days"))

    post_tbl = post_tbl.withColumnRenamed("person_id", "person_id_post") \
        .withColumnRenamed("post_window_start_dt", "post_window_start_dt_post")


    joined_df = post_tbl.join(covid_tbl, (post_tbl.person_id_post == covid_tbl.person_id) & ((post_tbl.post_window_start_dt_post == covid_tbl.post_window_start_dt)),
                              how="full_outer") \
        .select(
        coalesce(post_tbl.person_id_post, covid_tbl.person_id).alias("person_id"),
        coalesce(post_tbl.post_window_start_dt_post, covid_tbl.post_window_start_dt).alias("post_window_start_dt"),
        post_tbl.post_window_end_dt,
        post_tbl.post_tot_ip_days,
        covid_tbl.pre_window_end_dt,
        covid_tbl.covid_tot_ip_days
    )

    return joined_df


__all__ = [sql_statement_00, sql_statement_01, sql_statement_02, sql_statement_03, sql_statement_04, sql_statement_05,
           sql_statement_06, sql_statement_07, sql_statement_08, sql_statement_09, sql_statement_10]
