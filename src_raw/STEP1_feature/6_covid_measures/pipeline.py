

@transform_pandas(
    Output(rid="ri.vector.main.execute.1018f056-2996-47a5-949d-fcf8362a5a29"),
    pos_neg_date=Input(rid="ri.vector.main.execute.93733e19-4810-405f-90ae-5c17466940e8")
)
from pyspark.sql.functions import when, date_add, datediff, expr
def start_end_date(pos_neg_date):
    df = pos_neg_date
    df2 = df.withColumn("end_date", when(df.last_pos_dt.isNotNull() & df.first_neg_dt.isNotNull() & (df.first_neg_dt > df.last_pos_dt),
                                    expr("date_add(last_pos_dt, CAST(datediff(first_neg_dt, last_pos_dt)/2 AS INT))"))
                                    .when(df.last_pos_dt.isNotNull() & (df.first_neg_dt.isNull()), date_add(df.last_pos_dt, 3))
                                    .when(df.last_pos_dt.isNotNull() & df.first_neg_dt.isNotNull() & (df.first_neg_dt < df.last_pos_dt),
                                    date_add(df.last_pos_dt, 3))
                                    .otherwise(None))
    df2 = df2.withColumn('covid_length', datediff(df2.end_date, df2.first_pos_dt))
    df2 = df2.withColumn('impute_covid_length', when(df.last_pos_dt.isNotNull() & (df.first_neg_dt.isNull()), 1)
                                               .otherwise(0))
    return df2

