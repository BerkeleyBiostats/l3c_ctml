from pyspark.sql.functions import *

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
	# Join the two DataFrames on person_id
	joined_df = drug_exposure.join(Feature_table_builder, "person_id")

	# Select all columns from drug_exposure table
	df = joined_df.select([col("drug_exposure."+c) for c in drug_exposure.columns])
	return(df)

##@transform_pandas(
##    Output(rid="ri.vector.main.execute.130284d0-8168-4ecb-8556-9646aa90cd07"),
##    Feature_table_builder=Input(rid="ri.foundry.main.dataset.ce7a93a0-4140-4fdb-b97d-fb78c0caf345"),
##    drugRollUp=Input(rid="ri.vector.main.execute.685e760b-462c-47c5-b2b2-5e8d9b1b4973")
#)
def sql_statement_02(Feature_table_builder, drugRollUp):
	feat = Feature_table_builder
	co = drugRollUp

	# join Feature_table_builder to drugRollUp
	df = feat.join(co, (feat.person_id == co.person_id) & \
					(co.drug_exposure_start_date.between(feat.pre_window_end_dt, feat.post_window_start_dt)))

	# select all columns from Feature_table_builder and specific columns from drugRollUp
	df = df.select(feat.col("*"), 
				co.ancestor_drug_concept_name, 
				co.ancestor_drug_concept_id, 
				co.drug_exposure_start_date, 
				co.visit_occurrence_id)
	return(df)

##@transform_pandas(
##    Output(rid="ri.vector.main.execute.99e86a2a-69e5-4c25-b6e8-6ea4ade87b3f"),
##    Feature_table_builder=Input(rid="ri.foundry.main.dataset.ce7a93a0-4140-4fdb-b97d-fb78c0caf345"),
##    covid_drugs=Input(rid="ri.vector.main.execute.130284d0-8168-4ecb-8556-9646aa90cd07"),
##    microvisits_to_macrovisits=Input(rid="ri.foundry.main.dataset.d77a701f-34df-48a1-a71c-b28112a07ffa")
#)
def sql_statement_03(Feature_table_builder, covid_drugs, microvisits_to_macrovisits):
	feat = Feature_table_builder
	mml = microvisits_to_macrovisits
	prc = covid_drugs

	# join Feature_table_builder to microvisits_to_macrovisits and covid_drugs
	df = feat.join(mml, feat.person_id == mml.person_id)\
			.join(prc, mml.visit_occurrence_id == prc.visit_occurrence_id)

	# perform the select with calculated columns
	df = df.select(feat.person_id, 
				feat.patient_group, 
				prc.ancestor_drug_concept_name, 
				prc.ancestor_drug_concept_id, 
				lit("covid count").alias("count_type"), 
				countDistinct(coalesce(mml.macrovisit_id, mml.visit_occurrence_id)).alias("med_count"))\
			.groupBy(feat.person_id, 
					feat.patient_group, 
					prc.ancestor_drug_concept_name, 
					prc.ancestor_drug_concept_id)

	# select specific columns
	df = df.select(feat.person_id, 
				feat.patient_group, 
				prc.ancestor_drug_concept_name, 
				prc.ancestor_drug_concept_id, 
				lit("covid count").alias("count_type"), 
				countDistinct(coalesce(mml.macrovisit_id, mml.visit_occurrence_id)).alias("med_count"))
	return(df)

##@transform_pandas(
##    Output(rid="ri.vector.main.execute.685e760b-462c-47c5-b2b2-5e8d9b1b4973"),
##    DrugConcepts=Input(rid="ri.vector.main.execute.a63a8ea7-8537-4ef5-9e68-274c3b5bb545"),
##    Drugs_for_These_Patients=Input(rid="ri.vector.main.execute.288cdbd1-0d61-4a65-a86d-1714402f663f"),
##    concept_ancestor=Input(rid="ri.foundry.main.dataset.c5e0521a-147e-4608-b71e-8f53bcdbe03c")
#)
def sql_statement_04(DrugConcepts, Drugs_for_These_Patients, concept_ancestor):
	dc = DrugConcepts
	ca = concept_ancestor
	ds = Drugs_for_These_Patients

	# join DrugConcepts to concept_ancestor and Drugs_for_These_Patients
	df = dc.join(ca, dc.concept_id == ca.ancestor_concept_id)\
		.join(ds, ds.drug_concept_id == ca.descendant_concept_id)

	# select distinct columns
	df = df.selectDistinct(ds.person_id, 
						ds.drug_exposure_start_date, 
						ds.visit_occurrence_id, 
						ds.drug_concept_id.alias("original_drug_concept_id"), 
						ds.drug_concept_name.alias("original_drug_concept_name"), 
						ca.concept_id.alias("ancestor_drug_concept_id"), 
						ca.concept_name.alias("ancestor_drug_concept_name"))\
		.where((ca.ancestor_vocabulary_id == "RxNorm") & (ca.standard_concept == "S"))

	return(df)

##@transform_pandas(
##    Output(rid="ri.vector.main.execute.d7cd5658-cbb1-418e-9b84-a8777aa67f19"),
##    Feature_table_builder=Input(rid="ri.foundry.main.dataset.ce7a93a0-4140-4fdb-b97d-fb78c0caf345"),
##    drugRollUp=Input(rid="ri.vector.main.execute.685e760b-462c-47c5-b2b2-5e8d9b1b4973")
#)
def sql_statement_05(Feature_table_builder, drugRollUp):
	feat = Feature_table_builder
	co = drugRollUp

	# join Feature_table_builder to drugRollUp
	df = feat.join(co, (feat.person_id == co.person_id) & \
					(co.drug_exposure_start_date.between(feat.post_window_start_dt, feat.post_window_end_dt)))

	# select all columns from Feature_table_builder and specific columns from drugRollUp
	df = df.select(feat.col("*"), 
				co.ancestor_drug_concept_name, 
				co.ancestor_drug_concept_id, 
				co.drug_exposure_start_date, 
				co.visit_occurrence_id)
	return(df)

##@transform_pandas(
##    Output(rid="ri.vector.main.execute.f7b478eb-85f4-43a0-949c-2ecc78140e17"),
##    Feature_table_builder=Input(rid="ri.foundry.main.dataset.ce7a93a0-4140-4fdb-b97d-fb78c0caf345"),
##    microvisits_to_macrovisits=Input(rid="ri.foundry.main.dataset.d77a701f-34df-48a1-a71c-b28112a07ffa"),
##    post_drugs=Input(rid="ri.vector.main.execute.d7cd5658-cbb1-418e-9b84-a8777aa67f19")
#)
def sql_statement_06(Feature_table_builder, microvisits_to_macrovisits, post_drugs):
	feat = Feature_table_builder
	mml = microvisits_to_macrovisits
	prc = post_drugs

	# join Feature_table_builder to microvisits_to_macrovisits and post_drugs
	df = feat.join(mml, feat.person_id == mml.person_id)\
			.join(prc, mml.visit_occurrence_id == prc.visit_occurrence_id)

	# perform the select with calculated columns
	df = df.select(feat.person_id, 
				feat.patient_group, 
				prc.ancestor_drug_concept_name, 
				prc.ancestor_drug_concept_id, 
				lit("post count").alias("count_type"), 
				countDistinct(coalesce(mml.macrovisit_id, mml.visit_occurrence_id)).alias("med_count"))\
			.groupBy(feat.person_id, 
					feat.patient_group, 
					prc.ancestor_drug_concept_name, 
					prc.ancestor_drug_concept_id)

	# select specific columns
	df = df.select(feat.person_id, 
				feat.patient_group, 
				prc.ancestor_drug_concept_name, 
				prc.ancestor_drug_concept_id, 
				lit("post count").alias("count_type"), 
				countDistinct(coalesce(mml.macrovisit_id, mml.visit_occurrence_id)).alias("med_count"))
	return(df)

##@transform_pandas(
##    Output(rid="ri.vector.main.execute.6420351f-3985-4ec5-b098-66c21eb6900a"),
##    Feature_table_builder=Input(rid="ri.foundry.main.dataset.ce7a93a0-4140-4fdb-b97d-fb78c0caf345"),
##    drugRollUp=Input(rid="ri.vector.main.execute.685e760b-462c-47c5-b2b2-5e8d9b1b4973")
#)
def sql_statement_07(Feature_table_builder, drugRollUp):
	feat = Feature_table_builder
	co = drugRollUp

	# join Feature_table_builder to drugRollUp
	df = feat.join(co, (feat.person_id == co.person_id) & \
					(co.drug_exposure_start_date.between(feat.pre_window_start_dt, feat.pre_window_end_dt)))

	# select all columns from Feature_table_builder and specific columns from drugRollUp
	df = df.select(feat.col("*"), 
				co.ancestor_drug_concept_name, 
				co.ancestor_drug_concept_id, 
				co.drug_exposure_start_date, 
				co.visit_occurrence_id)
	return(df)

##@transform_pandas(
##    Output(rid="ri.vector.main.execute.3bd5ba7c-d0c2-4485-9d10-c16895794ea0"),
##    covidtbl=Input(rid="ri.vector.main.execute.99e86a2a-69e5-4c25-b6e8-6ea4ade87b3f"),
##    posttbl=Input(rid="ri.vector.main.execute.f7b478eb-85f4-43a0-949c-2ecc78140e17"),
##    prepretbl=Input(rid="ri.vector.main.execute.753f92de-1931-408e-ade4-9d18a7f4bb76"),
##    pretbl=Input(rid="ri.vector.main.execute.0c161a09-22c1-421b-b6ef-df510fa5d02c")
#)
def sql_statement_08(covidtbl, posttbl, prepretbl, pretbl):
	prepre = prepretbl.alias("prepre")
	pre = pretbl.alias("pre")
	covid = covidtbl.alias("covid")
	post = posttbl.alias("post")

	# perform the full outer join on prepre, pre, covid, and post tables
	df = prepre.join(pre, (prepre.person_id == pre.person_id) & (prepre.ancestor_drug_concept_name == pre.ancestor_drug_concept_name), how="full_outer")\
			.join(covid, (pre.person_id == covid.person_id) & (pre.ancestor_drug_concept_name == covid.ancestor_drug_concept_name), how="full_outer")\
			.join(post, (covid.person_id == post.person_id) & (covid.ancestor_drug_concept_name == post.ancestor_drug_concept_name), how="full_outer")

	# select specific columns
	df = df.select(prepre.person_id.alias("prepre_person_id"), 
				prepre.patient_group.alias("prepre_patient_group"), 
				prepre.ancestor_drug_concept_name.alias("prepre_ancestor_drug_concept_name"), 
				prepre.ancestor_drug_concept_id.alias("prepre_ancestor_drug_concept_id"), 
				prepre.count_type.alias("prepre_count_type"), 
				prepre.med_count.alias("prepre_med_count"), 
				pre.person_id.alias("pre_person_id"), 
				pre.patient_group.alias("pre_patient_group"), 
				pre.ancestor_drug_concept_name.alias("pre_ancestor_drug_concept_name"), 
				pre.ancestor_drug_concept_id.alias("pre_ancestor_drug_concept_id"), 
				pre.count_type.alias("pre_count_type"), 
				pre.med_count.alias("pre_med_count"), 
				covid.person_id.alias("covid_person_id"), 
				covid.patient_group.alias("covid_patient_group"), 
				covid.ancestor_drug_concept_name.alias("covid_ancestor_drug_concept_name"), 
				covid.ancestor_drug_concept_id.alias("covid_ancestor_drug_concept_id"), 
				covid.count_type.alias("covid_count_type"), 
				covid.med_count.alias("covid_med_count"), 
				post.person_id.alias("post_person_id"), 
				post.patient_group.alias("post_patient_group"), 
				post.ancestor_drug_concept_name.alias("post_ancestor_drug_concept_name"), 
				post.ancestor_drug_concept_id.alias("post_ancestor_drug_concept_id"), 
				post.count_type.alias("post_count_type"), 
				post.med_count.alias("post_med_count"))
	return(df)

##@transform_pandas(
##    Output(rid="ri.foundry.main.dataset.fa3fe17e-58ac-4615-a238-0fc24ecd9b6e"),
##    Feature_table_builder=Input(rid="ri.foundry.main.dataset.ce7a93a0-4140-4fdb-b97d-fb78c0caf345"),
##    pre_post_med_count=Input(rid="ri.vector.main.execute.3bd5ba7c-d0c2-4485-9d10-c16895794ea0")
#)
def sql_statement_09(Feature_table_builder, pre_post_med_count):
	feat_tbl = Feature_table_builder

	# Join Feature_table_builder with pre_post_med_count table
	feat_tbl = feat_tbl.join(pre_post_med_count, pre_post_med_count.person_id == feat_tbl.person_id, 'inner') \
						.select(pre_post_med_count.prepre_person_id, pre_post_med_count.prepre_patient_group,
								pre_post_med_count.prepre_ancestor_drug_concept_name, pre_post_med_count.prepre_ancestor_drug_concept_id,
								pre_post_med_count.prepre_count_type, pre_post_med_count.prepre_med_count,
								pre_post_med_count.pre_person_id, pre_post_med_count.pre_patient_group,
								pre_post_med_count.pre_ancestor_drug_concept_name, pre_post_med_count.pre_ancestor_drug_concept_id,
								pre_post_med_count.pre_count_type, pre_post_med_count.pre_med_count,
								pre_post_med_count.covid_person_id, pre_post_med_count.covid_patient_group,
								pre_post_med_count.covid_ancestor_drug_concept_name, pre_post_med_count.covid_ancestor_drug_concept_id,
								pre_post_med_count.covid_count_type, pre_post_med_count.covid_med_count,
								pre_post_med_count.post_person_id, pre_post_med_count.post_patient_group,
								pre_post_med_count.post_ancestor_drug_concept_name, pre_post_med_count.post_ancestor_drug_concept_id,
								pre_post_med_count.post_count_type, pre_post_med_count.post_med_count,
								feat_tbl.apprx_age, feat_tbl.sex, feat_tbl.race, feat_tbl.ethn,
								feat_tbl.tot_long_data_days, feat_tbl.op_post_visit_ratio, feat_tbl.post_ip_visit_ratio,
								feat_tbl.covid_ip_visit_ratio, feat_tbl.post_icu_visit_ratio, feat_tbl.covid_icu_visit_ratio)

	# Replace null values with 0 for the medication counts
	feat_tbl = feat_tbl.fillna({'prepre_med_count': 0, 'pre_med_count': 0, 'covid_med_count': 0, 'post_med_count': 0})

	# Group by the necessary columns and compute medication count
	med_count = feat_tbl.groupby('person_id', 'patient_group', 'ancestor_drug_concept_name', 'ancestor_drug_concept_id', 'count_type') \
						.agg(countDistinct(when(col('count_type') == 'prepre', col('prepre_med_count'))
										.when(col('count_type') == 'pre', col('pre_med_count'))
										.when(col('count_type') == 'covid', col('covid_med_count'))
										.when(col('count_type') == 'post', col('post_med_count'))
										.otherwise(None)).alias('med_count'))

	# Rename the columns to match the expected output
	med_count = med_count.selectExpr('person_id', 'patient_group', 'ancestor_drug_concept_name', 'ancestor_drug_concept_id', 
									"'covid count' as count_type", 'med_count')

	return(med_count)

##@transform_pandas(
##    Output(rid="ri.vector.main.execute.9b9a05ee-943a-4764-9585-7f94c813af83"),
##    Feature_table_builder=Input(rid="ri.foundry.main.dataset.ce7a93a0-4140-4fdb-b97d-fb78c0caf345"),
##    drugRollUp=Input(rid="ri.vector.main.execute.685e760b-462c-47c5-b2b2-5e8d9b1b4973")
#)
def sql_statement_10(Feature_table_builder, drugRollUp):
	feat = Feature_table_builder
	df = feat.join(drugRollUp, (feat.person_id == drugRollUp.person_id) & \
				(drugRollUp.drug_exposure_start_date.between(feat.pre_pre_window_start_dt, feat.pre_window_start_dt))) \
			.select(feat.person_id, feat.patient_group, drugRollUp.ancestor_drug_concept_name,
					drugRollUp.ancestor_drug_concept_id, drugRollUp.drug_exposure_start_date,
					drugRollUp.visit_occurrence_id)
	return(df)

##@transform_pandas(
##    Output(rid="ri.vector.main.execute.753f92de-1931-408e-ade4-9d18a7f4bb76"),
##    Feature_table_builder=Input(rid="ri.foundry.main.dataset.ce7a93a0-4140-4fdb-b97d-fb78c0caf345"),
##    microvisits_to_macrovisits=Input(rid="ri.foundry.main.dataset.d77a701f-34df-48a1-a71c-b28112a07ffa"),
##    pre_pre_drugs=Input(rid="ri.vector.main.execute.9b9a05ee-943a-4764-9585-7f94c813af83")
#)
def sql_statement_11(Feature_table_builder, microvisits_to_macrovisits, pre_pre_drugs):
	feat = Feature_table_builder
	mml = microvisits_to_macrovisits
	prc = pre_pre_drugs

	df = feat.join(mml, "person_id").join(prc, "visit_occurrence_id")
	df = df.select(
		col("feat.person_id"),
		col("feat.patient_group"),
		col("prc.ancestor_drug_concept_name"),
		col("prc.ancestor_drug_concept_id"),
		concat(lit("pre pre count"), lit(" ").cast("string")).alias("count_type"),
		countDistinct(when(col("mml.macrovisit_id").isNotNull(), col("mml.macrovisit_id")).otherwise(col("mml.visit_occurrence_id"))).alias("med_count")
	).groupBy(
		col("feat.person_id"),
		col("feat.patient_group"),
		col("prc.ancestor_drug_concept_name"),
		col("prc.ancestor_drug_concept_id")
	).agg(
		{"med_count": "sum"}
	)
	return(df)

##@transform_pandas(
##    Output(rid="ri.vector.main.execute.0c161a09-22c1-421b-b6ef-df510fa5d02c"),
##    Feature_table_builder=Input(rid="ri.foundry.main.dataset.ce7a93a0-4140-4fdb-b97d-fb78c0caf345"),
##    microvisits_to_macrovisits=Input(rid="ri.foundry.main.dataset.d77a701f-34df-48a1-a71c-b28112a07ffa"),
##    pre_drugs=Input(rid="ri.vector.main.execute.6420351f-3985-4ec5-b098-66c21eb6900a")
#)
def sql_statement_12(Feature_table_builder, microvisits_to_macrovisits, pre_drugs):
	df = feat.join(mml, "person_id").join(prc, "visit_occurrence_id")
	df = df.select(
		col("feat.person_id"),
		col("feat.patient_group"),
		col("prc.ancestor_drug_concept_name"),
		col("prc.ancestor_drug_concept_id"),
		lit("pre count").alias("count_type"),
		countDistinct(when(col("mml.macrovisit_id").isNotNull(), col("mml.macrovisit_id")).otherwise(col("mml.visit_occurrence_id"))).alias("med_count")
	).groupBy(
		col("feat.person_id"),
		col("feat.patient_group"),
		col("prc.ancestor_drug_concept_name"),
		col("prc.ancestor_drug_concept_id")
	).agg(
		{"med_count": "sum"}
	)
	return(df)

