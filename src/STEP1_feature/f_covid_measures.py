

###@transform_pandas(
###    Output(rid="ri.vector.main.execute.1018f056-2996-47a5-949d-fcf8362a5a29"),
###    pos_neg_date=Input(rid="ri.vector.main.execute.93733e19-4810-405f-90ae-5c17466940e8")
#)
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

def sql_statement_00():
	      statement = '''SELECT\ nvl\(covidtbl\.person_id,\ posttbl\.person_id\)\ as\ person_id,\ nvl\(covidtbl\.measure_type,\ posttbl\.measure_type\)\ as\ measure_type,\ covidtbl\.c_any_pos,\ covidtbl\.c_any_measure,\ posttbl\.post_any_pos,\ posttbl\.post_any_measure\
\
FROM\
\-\-\ acute\ covid\ window\
\ \ \ \ \(SELECT\ distinct\ c\.person_id,\ c\.measure_type,\ max\(c\.pos\)\ as\ c_any_pos,\ 1\ as\ c_any_measure\
\ \ \ \ FROM\
\ \ \ \ \ \ \ \ \(SELECT\ cp\.person_id,\ cp\.measurement_concept_name\ as\ measure_type,\ pos_or_neg\ as\ pos\
\ \ \ \ \ \ \ \ FROM\ covid_person\ cp\
\ \ \ \ \ \ \ \ WHERE\ cp\.measurement_date\ >\ cp\.pre_window_end_dt\ and\ cp\.measurement_date\ <=\ cp\.post_window_start_dt\
\ \ \ \ \ \ \ \ \)\ as\ c\ \
\ \ \ \ GROUP\ BY\ c\.person_id,\ c\.measure_type\)\ covidtbl\
\
FULL\ JOIN\ \
\
\-\-\ post\ covid\ window\
\ \ \ \ \(SELECT\ distinct\ c\.person_id,\ c\.measure_type,\ max\(c\.pos\)\ as\ post_any_pos,\ 1\ as\ post_any_measure\
\ \ \ \ FROM\
\ \ \ \ \ \ \ \ \(SELECT\ cp\.person_id,\ cp\.measurement_concept_name\ as\ measure_type,\ pos_or_neg\ as\ pos\
\ \ \ \ \ \ \ \ FROM\ covid_person\ cp\
\ \ \ \ \ \ \ \ WHERE\ cp\.measurement_date\ >\ cp\.post_window_start_dt\ and\ cp\.measurement_date\ <=\ cp\.post_window_end_dt\
\ \ \ \ \ \ \ \ \)\ as\ c\ \
\ \ \ \ GROUP\ BY\ c\.person_id,\ c\.measure_type\)\ posttbl\
\
ON\ covidtbl\.person_id\ =\ posttbl\.person_id\ AND\ covidtbl\.measure_type\ =\ posttbl\.measure_type\
'''
	      return(statement)

##@transform_pandas(
##    Output(rid="ri.foundry.main.dataset.5b072ea2-72c8-42d1-b653-1cfaf691857b"),
##    covid_measure_indicators=Input(rid="ri.vector.main.execute.7c00f4ba-988d-44b7-ace8-3792fc7d61dd"),
##    start_end_date=Input(rid="ri.vector.main.execute.1018f056-2996-47a5-949d-fcf8362a5a29")
#)
def sql_statement_01():
	      statement = '''SELECT\ nvl\(cmi\.person_id,\ sed\.person_id\)\ as\ person_id,\ nvl\(cmi\.measure_type,\ sed\.measure_type\)\ as\ measure_type,\ \
\ \ \ \ \ \ \ \ cmi\.c_any_measure,\ cmi\.c_any_pos,\ sed\.c_covid_length,\ sed\.c_impute_covid_length,\
\ \ \ \ \ \ \ \ cmi\.post_any_measure,\ cmi\.post_any_pos,\ sed\.post_covid_length,\ sed\.post_impute_covid_length\
\
FROM\ \
\
\ \ \ \ covid_measure_indicators\ cmi\ \
\
\ \ \ \ FULL\ JOIN\ \
\
\ \ \ \ \(SELECT\ nvl\(covidtbl\.person_id,\ posttbl\.person_id\)\ as\ person_id,\ nvl\(covidtbl\.measure_type,\ posttbl\.measure_type\)\ as\ measure_type,\ \
\ \ \ \ \ \ \ \ \ \ \ \ covidtbl\.c_covid_length,\ covidtbl\.c_impute_covid_length,\ posttbl\.post_covid_length,\ posttbl\.post_impute_covid_length\
\
\ \ \ \ FROM\
\ \ \ \ \ \ \ \ \(SELECT\ distinct\ person_id,\ measure_type,\ covid_length\ as\ c_covid_length,\ impute_covid_length\ as\ c_impute_covid_length\
\ \ \ \ \ \ \ \ FROM\ start_end_date\ sed\
\ \ \ \ \ \ \ \ WHERE\ sed\.window_type\ ==\ "covid"\)\ covidtbl\
\
\ \ \ \ \ \ \ \ FULL\ JOIN\
\
\ \ \ \ \ \ \ \ \(SELECT\ distinct\ person_id,\ measure_type,\ covid_length\ as\ post_covid_length,\ impute_covid_length\ as\ post_impute_covid_length\
\ \ \ \ \ \ \ \ FROM\ start_end_date\ sed\
\ \ \ \ \ \ \ \ WHERE\ sed\.window_type\ ==\ "pos_covid"\)\ posttbl\
\
\ \ \ \ \ \ \ \ ON\ covidtbl\.person_id\ =\ posttbl\.person_id\ AND\ covidtbl\.measure_type\ =\ posttbl\.measure_type\
\ \ \ \ \)\ sed\ \
\
ON\ cmi\.person_id\ =\ sed\.person_id\ AND\ cmi\.measure_type\ =\ sed\.measure_type\
'''
	      return(statement)

##@transform_pandas(
##    Output(rid="ri.vector.main.execute.23ea3189-2921-45e1-9782-ac15dfb58c8b"),
##    Feature_table_builder=Input(rid="ri.foundry.main.dataset.ce7a93a0-4140-4fdb-b97d-fb78c0caf345"),
##    measurement=Input(rid="ri.foundry.main.dataset.5c8b84fb-814b-4ee5-a89a-9525f4a617c7")
#)
def sql_statement_02(Feature_table_builder, measurement, concept):
	ft = Feature_table_builder
	m = measurement.join(concept,(measurement.measurement_concept_id == concept.concept_id), 'left')
	df = ft.join(m, ft.person_id == m.person_id) \
		.where(m.value_as_concept_id.isin("Not detected", "Detected", "Positive",
											"Negative") & m.measurement_date.isNotNull() & m.concept_name.isin(
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
		.groupBy("person_id", "measurement_date", "m.concept_name") \
		.agg(max(when(m.value_as_concept_id.isin("Detected", "Positive"), 1).otherwise(0)).alias("pos_or_neg"), \
			 max(ft.pre_window_end_dt).alias("pre_window_end_dt"), \
			 max(ft.post_window_start_dt).alias("post_window_start_dt"), \
			 max(ft.post_window_end_dt).alias("post_window_end_dt"))
	return df

##@transform_pandas(
##    Output(rid="ri.vector.main.execute.b27c552d-ec9a-48c8-b742-27e8483a88cb"),
##    covid_person=Input(rid="ri.vector.main.execute.23ea3189-2921-45e1-9782-ac15dfb58c8b")
#)
def sql_statement_03(covid_person):
	df = covid_person.selectExpr(
		"person_id",
		"concept_name as measure_type",
		when(covid_person.pos_or_neg == 1, covid_person.measurement_date).otherwise(None).alias("measure_pos_date"),
		when(covid_person.pos_or_neg == 0, covid_person.measurement_date).otherwise(None).alias("measure_neg_date")
	).filter((covid_person.measurement_date > covid_person.pre_window_end_dt) & (
				covid_person.measurement_date <= covid_person.post_window_start_dt))
	return df

##@transform_pandas(
##    Output(rid="ri.vector.main.execute.93733e19-4810-405f-90ae-5c17466940e8"),
##    covid_window=Input(rid="ri.vector.main.execute.b27c552d-ec9a-48c8-b742-27e8483a88cb"),
##    post_covid=Input(rid="ri.vector.main.execute.f8eeed48-0ebb-4e6a-9f4f-8cccc7fa3914")
#)
def sql_statement_04():
	      statement = '''\-\-\ first\ \(min\)\ postive\ date,\ last\ \(max\)\ postive\ date,\ first\ negative\ date\ after\ the\ first\ positive\ date,\ number\ of\ tests\
\-\-\ covid\
SELECT\ t\.\*,\ min\(c\.measure_neg_date\)\ as\ first_neg_dt,\ 'covid'\ as\ window_type\
FROM\ covid_window\ c\
LEFT\ JOIN\ \
\(SELECT\ c\.person_id,\ c\.measure_type,\ \
\ \ \ \ min\(measure_pos_date\)\ as\ first_pos_dt,\
\ \ \ \ max\(measure_pos_date\)\ as\ last_pos_dt\
FROM\ covid_window\ c\
GROUP\ BY\ c\.person_id,\ c\.measure_type\)\ t\
ON\ c\.person_id\ =\ t\.person_id\ AND\ c\.measure_type\ =\ t\.measure_type\
WHERE\ c\.measure_neg_date\ >\ t\.first_pos_dt\ \-\-\ first\ negative\ date\ after\ the\ positive\ date\
\ \ \ \ OR\ c\.measure_neg_date\ IS\ NULL\ \
GROUP\ BY\ t\.person_id,\ t\.measure_type,\ t\.first_pos_dt,\ t\.last_pos_dt\
\
UNION\
\-\-\ post\-covid\
SELECT\ t\.\*,\ min\(p\.measure_neg_date\)\ as\ first_neg_dt,\ 'pos_covid'\ as\ window_type\
FROM\ post_covid\ p\
LEFT\ JOIN\ \
\(SELECT\ p\.person_id,\ p\.measure_type,\ \
\ \ \ \ min\(measure_pos_date\)\ as\ first_pos_dt,\
\ \ \ \ max\(measure_pos_date\)\ as\ last_pos_dt\
FROM\ post_covid\ p\
GROUP\ BY\ p\.person_id,\ p\.measure_type\)\ t\
ON\ p\.person_id\ =\ t\.person_id\ AND\ p\.measure_type\ =\ t\.measure_type\
WHERE\ p\.measure_neg_date\ >\ t\.first_pos_dt\ \-\-\ first\ negative\ date\ after\ the\ positive\ date\
\ \ \ \ OR\ p\.measure_neg_date\ IS\ NULL\ \
GROUP\ BY\ t\.person_id,\ t\.measure_type,\ t\.first_pos_dt,\ t\.last_pos_dt\
'''
	      return(statement)

##@transform_pandas(
##    Output(rid="ri.vector.main.execute.f8eeed48-0ebb-4e6a-9f4f-8cccc7fa3914"),
##    covid_person=Input(rid="ri.vector.main.execute.23ea3189-2921-45e1-9782-ac15dfb58c8b")
#)
def sql_statement_05(covid_person):
	df = covid_person.selectExpr(
		"person_id",
		"concept_name as measure_type",
		when(covid_person.pos_or_neg == 1, covid_person.measurement_date).otherwise(None).alias("measure_pos_date"),
		when(covid_person.pos_or_neg == 0, covid_person.measurement_date).otherwise(None).alias("measure_neg_date")
	).filter((covid_person.measurement_date > covid_person.post_window_start_dt) & (
			covid_person.measurement_date <= covid_person.post_window_end_dt))
	return df
