from pyspark.sql.functions import *


def start_end_date(pos_neg_date):
    df = pos_neg_date
    df2 = df.withColumn("end_date", when
    (df.last_pos_dt.isNotNull() & df.first_neg_dt.isNotNull() & (df.first_neg_dt > df.last_pos_dt),
     expr("date_add(last_pos_dt, CAST(datediff(first_neg_dt, last_pos_dt)/2 AS INT))"))
                        .when(df.last_pos_dt.isNotNull() & (df.first_neg_dt.isNull()), date_add(df.last_pos_dt, 3))
                        .when
    (df.last_pos_dt.isNotNull() & df.first_neg_dt.isNotNull() & (df.first_neg_dt < df.last_pos_dt),
     date_add(df.last_pos_dt, 3))
                        .otherwise(None))
    df2 = df2.withColumn('covid_length', datediff(df2.end_date, df2.first_pos_dt))
    df2 = df2.withColumn('impute_covid_length', when(df.last_pos_dt.isNotNull() & (df.first_neg_dt.isNull()), 1)
                         .otherwise(0))
    return df2


def sql_statement_00(covid_person):
    c = covid_person.filter((covid_person.measurement_date > covid_person.pre_window_end_dt) &
                            (covid_person.measurement_date <= covid_person.post_window_start_dt)) \
        .select(col("person_id"), col("concept_name").alias("measure_type"), col("pos_or_neg").alias("pos"))

    covidtbl = c.select(col("person_id"), col("measure_type"), col("pos").alias("c_any_pos"),
                        lit(1).alias("c_any_measure")) \
        .groupBy("person_id", "measure_type") \
        .agg(max("c_any_pos").alias("c_any_pos"), max("c_any_measure").alias("c_any_measure"))

    p = covid_person.filter((covid_person.measurement_date > covid_person.post_window_start_dt) &
                            (covid_person.measurement_date <= covid_person.post_window_end_dt)) \
        .select(col("person_id"), col("concept_name").alias("measure_type"), col("pos_or_neg").alias("pos"))

    posttbl = p.select(col("person_id"), col("measure_type"), col("pos").alias("post_any_pos"),
                       lit(1).alias("post_any_measure")) \
        .groupBy("person_id", "measure_type") \
        .agg(max("post_any_pos").alias("post_any_pos"), max("post_any_measure").alias("post_any_measure"))

    joined_df = covidtbl.join(posttbl, ["person_id", "measure_type"], "fullouter") \
        .select("person_id",
                "measure_type",
                covidtbl.c_any_pos, covidtbl.c_any_measure,
                posttbl.post_any_pos, posttbl.post_any_measure)
    return joined_df


def sql_statement_01(covid_measure_indicators, start_end_date_df):
    cmi = covid_measure_indicators
    covidtbl = start_end_date_df.filter(col("window_type") == "covid") \
        .select("person_id", "measure_type",
                col("covid_length").alias("c_covid_length"),
                col("impute_covid_length").alias("c_impute_covid_length")) \
        .distinct()

    posttbl = start_end_date_df.filter(col("window_type") == "pos_covid") \
        .select("person_id", "measure_type",
                col("covid_length").alias("post_covid_length"),
                col("impute_covid_length").alias("post_impute_covid_length")) \
        .distinct()

    sed = covidtbl.join(posttbl, ['person_id', 'measure_type'], how='full') \
        .select(
        "person_id",
        "measure_type",
        covidtbl.c_covid_length,
        covidtbl.c_impute_covid_length,
        posttbl.post_covid_length,
        posttbl.post_impute_covid_length
    )

    result = cmi.join(
        sed,
        on=["person_id", "measure_type"],
        how="full"
    ).select(
        "person_id",
        "measure_type",
        cmi.c_any_measure,
        cmi.c_any_pos,
        sed.c_covid_length,
        sed.c_impute_covid_length,
        cmi.post_any_measure,
        cmi.post_any_pos,
        sed.post_covid_length,
        sed.post_impute_covid_length
    )
    return result


def sql_statement_02(Feature_table_builder, measurement, concept):
    ft = Feature_table_builder
    m = measurement.join(concept, (measurement.measurement_concept_id == concept.concept_id), 'left')
    df = ft.join(m, 'person_id', 'left') \
        .where(m.value_as_concept_id.isin([45880296, 9190,  # Not detected
                                           4126681, 45877985,  # Detected
                                           45884084, 9191,  # Positive
                                           45878583, 9189]) & m.measurement_date.isNotNull() & col("concept_name").isin(
        "SARS-CoV-2 (COVID-19) RNA [Presence] in Respiratory specimen by NAA with probe detection",
        "SARS-CoV-2 (COVID-19) RNA [Presence] in Specimen by NAA with probe detection",
        "SARS-CoV-2 (COVID-19) N gene [Presence] in Specimen by Nucleic acid amplification using CDC primer-probe set N1",
        "SARS-CoV-2 (COVID-19) ORF1ab region [Presence] in Respiratory specimen by NAA with probe detection",
        "SARS-CoV-2 (COVID-19) Ag [Presence] in Respiratory specimen by Rapid immunoassay",
        "SARS-CoV-2 (COVID-19) RdRp gene [Presence] in Respiratory specimen by NAA with probe detection",
        "SARS-CoV-2 (COVID-19) RdRp gene [Presence] in Specimen by NAA with probe detection",
        "SARS-CoV+SARS-CoV-2 (COVID-19) Ag [Presence] in Respiratory specimen by Rapid immunoassay",
        "SARS-CoV-2 (COVID-19) RNA panel - Specimen by NAA with probe detection",
        "SARS-CoV-2 (COVID-19) RNA [Presence] in Nasopharynx by NAA with non-probe detection",
        "SARS-CoV-2 (COVID-19) N gene [Presence] in Specimen by NAA with probe detection",
        "SARS-CoV-2 (COVID-19) IgG Ab [Presence] in Serum or Plasma by Immunoassay")) \
        .groupBy("person_id", "measurement_date", "concept_name") \
        .agg(
        max(when(m.value_as_concept_id.isin([4126681, 45877985, 45884084, 9191]), 1).otherwise(0)).alias("pos_or_neg"), \
        max(ft.pre_window_end_dt).alias("pre_window_end_dt"), \
        max(ft.post_window_start_dt).alias("post_window_start_dt"), \
        max(ft.post_window_end_dt).alias("post_window_end_dt"))
    return df


def sql_statement_03(covid_person):
    df = covid_person \
        .filter(
        (col("measurement_date") > col("pre_window_end_dt")) & (col("measurement_date") <= col("post_window_start_dt"))) \
        .select(
        col("person_id"),
        col("concept_name").alias("measure_type"),
        when(col("pos_or_neg") == 1, col("measurement_date")).otherwise(None).alias("measure_pos_date"),
        when(col("pos_or_neg") == 0, col("measurement_date")).otherwise(None).alias("measure_neg_date")
    )
    return df


def sql_statement_04(covid_window, post_covid):
    ct = covid_window.groupBy("person_id", "measure_type") \
        .agg(min("measure_pos_date").alias("first_pos_dt"), max("measure_pos_date").alias("last_pos_dt"))

    covidtbl = (covid_window
                .join(ct, ["person_id", "measure_type"], "left")
                .where((col("measure_neg_date") > col("first_pos_dt")) | (col("measure_neg_date").isNull()))
                .groupBy("person_id", "measure_type", "first_pos_dt", "last_pos_dt")
                .agg(min(col("measure_neg_date")).alias("first_neg_dt"),
                     lit('covid').alias("window_type")))

    t = post_covid.groupBy("person_id", "measure_type") \
        .agg(min("measure_pos_date").alias("first_pos_dt"), max("measure_pos_date").alias("last_pos_dt"))

    posttbl = (post_covid
               .join(t, ["person_id", "measure_type"], "left")
               .where((col("measure_neg_date") > col("first_pos_dt")) | (col("measure_neg_date").isNull()))
               .groupBy("person_id", "measure_type", "first_pos_dt", "last_pos_dt")
               .agg(min(col("measure_neg_date")).alias("first_neg_dt"),
                    lit('pos_covid').alias("window_type")))
    df = covidtbl.union(posttbl)
    return df


def sql_statement_05(covid_person):
    df = covid_person \
        .filter(
        (col("measurement_date") > col("post_window_start_dt")) & (
                col("measurement_date") <= col("post_window_end_dt"))) \
        .select(
        col("person_id"),
        col("concept_name").alias("measure_type"),
        when(col("pos_or_neg") == 1, col("measurement_date")).otherwise(None).alias("measure_pos_date"),
        when(col("pos_or_neg") == 0, col("measurement_date")).otherwise(None).alias("measure_neg_date")
    )
    return df
