

@transform_pandas(
    Output(rid="ri.vector.main.execute.d23e00b9-b656-4f87-8374-682fce8bd41d"),
    Feature_table_builder=Input(rid="ri.foundry.main.dataset.ce7a93a0-4140-4fdb-b97d-fb78c0caf345"),
    condition_occurrence=Input(rid="ri.foundry.main.dataset.2f496793-6a4e-4bf4-b0fc-596b277fb7e2")
)
-- find all conditions associated with patients in their acute covid window
SELECT feat.*, co.condition_concept_name, co.condition_concept_id, co.condition_start_date, co.condition_source_value, co.visit_occurrence_id
FROM Feature_table_builder feat 
JOIN 
condition_occurrence co ON feat.person_id = co.person_id and co.condition_start_date between feat.pre_window_end_dt and feat.post_window_start_dt

@transform_pandas(
    Output(rid="ri.vector.main.execute.2509036e-28dc-4fcc-9206-9aeb7ad296e1"),
    Feature_table_builder=Input(rid="ri.foundry.main.dataset.ce7a93a0-4140-4fdb-b97d-fb78c0caf345"),
    covid_condition=Input(rid="ri.vector.main.execute.d23e00b9-b656-4f87-8374-682fce8bd41d"),
    microvisits_to_macrovisits=Input(rid="ri.foundry.main.dataset.d77a701f-34df-48a1-a71c-b28112a07ffa"),
    post_condition=Input(rid="ri.vector.main.execute.e622f396-e4d0-4b55-83b7-f9d2c4682b7b"),
    pre_condition=Input(rid="ri.vector.main.execute.b71fb942-8c0b-4f2d-8c77-1d2c6c0cfcfe"),
    pre_pre_condition=Input(rid="ri.vector.main.execute.052c1047-92d1-4c5a-96b5-c39efb023e6e")
)
-- full outer join pre and post in order to compare
SELECT 
prepretbl.person_id as pre_pre_person_id, prepretbl.patient_group as pre_pre_patient_group, prepretbl.condition_concept_name as pre_pre_condition_concept_name, prepretbl.condition_concept_id as pre_pre_condition_concept_id, prepretbl.count_type as pre_pre_count_type, prepretbl.dx_count as pre_pre_dx_count,
pretbl.person_id as pre_person_id, pretbl.patient_group as pre_patient_group, pretbl.condition_concept_name as pre_condition_concept_name, pretbl.condition_concept_id as pre_condition_concept_id, pretbl.count_type as pre_count_type, pretbl.dx_count as pre_dx_count,
covidtbl.person_id as covid_person_id, covidtbl.patient_group as covid_patient_group, covidtbl.condition_concept_name as covid_condition_concept_name, covidtbl.condition_concept_id as covid_condition_concept_id, covidtbl.count_type as covid_count_type, covidtbl.dx_count as covid_dx_count,
posttbl.person_id as post_person_id, posttbl.patient_group as post_patient_group, posttbl.condition_concept_name as post_condition_concept_name, posttbl.condition_concept_id as post_condition_concept_id, posttbl.count_type as post_count_type, posttbl.dx_count as post_dx_count
FROM 
	(SELECT feat.person_id, feat.patient_group, prc.condition_concept_name, prc.condition_concept_id, 'pre count' as 
		count_type, count(distinct nvl(mml.macrovisit_id, mml.visit_occurrence_id)) as dx_count
	FROM Feature_table_builder feat 
    		JOIN microvisits_to_macrovisits mml ON feat.person_id = mml.person_id
    		JOIN pre_pre_condition prc ON mml.visit_occurrence_id = prc.visit_occurrence_id
	GROUP BY 
	feat.person_id, feat.patient_group, prc.condition_concept_name, prc.condition_concept_id
	) prepretbl

FULL JOIN

	(SELECT feat.person_id, feat.patient_group, prc.condition_concept_name, prc.condition_concept_id, 'pre count' as 
		count_type, count(distinct nvl(mml.macrovisit_id, mml.visit_occurrence_id)) as dx_count
	FROM Feature_table_builder feat 
    		JOIN microvisits_to_macrovisits mml ON feat.person_id = mml.person_id
    		JOIN pre_condition prc ON mml.visit_occurrence_id = prc.visit_occurrence_id
	GROUP BY 
	feat.person_id, feat.patient_group, prc.condition_concept_name, prc.condition_concept_id
	) pretbl
ON 
	pretbl.person_id = prepretbl.person_id AND pretbl.condition_concept_name = prepretbl.condition_concept_name

FULL JOIN

	(SELECT feat.person_id, feat.patient_group, prc.condition_concept_name, prc.condition_concept_id, 'pre count' as 
		count_type, count(distinct nvl(mml.macrovisit_id, mml.visit_occurrence_id)) as dx_count
	FROM Feature_table_builder feat 
    		JOIN microvisits_to_macrovisits mml ON feat.person_id = mml.person_id
    		JOIN covid_condition prc ON mml.visit_occurrence_id = prc.visit_occurrence_id
	GROUP BY 
	feat.person_id, feat.patient_group, prc.condition_concept_name, prc.condition_concept_id
	) covidtbl
ON 
	pretbl.person_id = covidtbl.person_id AND pretbl.condition_concept_name = covidtbl.condition_concept_name

FULL JOIN

	(SELECT feat.person_id, feat.patient_group, prc.condition_concept_name, prc.condition_concept_id, 'post count' as 
		count_type, count(distinct nvl(mml.macrovisit_id, mml.visit_occurrence_id)) as dx_count
	FROM Feature_table_builder feat 
    		JOIN microvisits_to_macrovisits mml ON feat.person_id = mml.person_id
    		JOIN post_condition prc ON mml.visit_occurrence_id = prc.visit_occurrence_id
	GROUP BY 
	feat.person_id, feat.patient_group, prc.condition_concept_name, prc.condition_concept_id
    ) posttbl 
    ON 
	pretbl.person_id = posttbl.person_id AND pretbl.condition_concept_name = posttbl.condition_concept_name

@transform_pandas(
    Output(rid="ri.vector.main.execute.e622f396-e4d0-4b55-83b7-f9d2c4682b7b"),
    Feature_table_builder=Input(rid="ri.foundry.main.dataset.ce7a93a0-4140-4fdb-b97d-fb78c0caf345"),
    condition_occurrence=Input(rid="ri.foundry.main.dataset.2f496793-6a4e-4bf4-b0fc-596b277fb7e2")
)
-- find all conditions associated with patients in their post window
SELECT feat.*, co.condition_concept_name, co.condition_concept_id, co.condition_start_date, co.condition_source_value, co.visit_occurrence_id
FROM Feature_table_builder feat 
JOIN 
condition_occurrence co ON feat.person_id = co.person_id and co.condition_start_date between feat.post_window_start_dt and feat.post_window_end_dt

@transform_pandas(
    Output(rid="ri.vector.main.execute.b71fb942-8c0b-4f2d-8c77-1d2c6c0cfcfe"),
    Feature_table_builder=Input(rid="ri.foundry.main.dataset.ce7a93a0-4140-4fdb-b97d-fb78c0caf345"),
    condition_occurrence=Input(rid="ri.foundry.main.dataset.2f496793-6a4e-4bf4-b0fc-596b277fb7e2")
)
-- find all conditions associated with patients in their pre window
SELECT feat.*, co.condition_concept_name, co.condition_concept_id, co.condition_start_date, co.condition_source_value, co.visit_occurrence_id
FROM Feature_table_builder feat 
JOIN 
condition_occurrence co ON feat.person_id = co.person_id and co.condition_start_date between feat.pre_window_start_dt and feat.pre_window_end_dt

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.993922b2-c5f2-4508-9ec7-b25aaa0c9750"),
    Feature_table_builder=Input(rid="ri.foundry.main.dataset.ce7a93a0-4140-4fdb-b97d-fb78c0caf345"),
    four_windows_dx_counts=Input(rid="ri.vector.main.execute.2509036e-28dc-4fcc-9206-9aeb7ad296e1")
)
-- clean up the full outer join for meds. this table will be handed off as is to the next part of the pipeline
-- STOP: The results of the query below need to be stored in a table called "pre_post_dx_count_clean". Once you have this table created, move to the next script in the sequence.
select distinct tbl.*, feat.apprx_age, feat.sex, feat.race, feat.ethn, feat.tot_long_data_days, feat.op_post_visit_ratio as op_post_visit_ratio, feat.post_ip_visit_ratio, feat.covid_ip_visit_ratio, feat.post_icu_visit_ratio, feat.covid_icu_visit_ratio
FROM
Feature_table_builder feat JOIN
	(SELECT 
	nvl(nvl(nvl(pre_pre_person_id, pre_person_id), covid_person_id), post_person_id) as person_id, 
    nvl(nvl(nvl(pre_pre_patient_group, pre_patient_group), covid_patient_group), post_patient_group) as patient_group, 
	nvl(nvl(nvl(pre_pre_condition_concept_name, pre_condition_concept_name), covid_condition_concept_name), post_condition_concept_name) as condition_concept_name, 
	nvl(nvl(nvl(pre_pre_condition_concept_id, pre_condition_concept_id), covid_condition_concept_id), post_condition_concept_id) as condition_concept_id, 
    nvl(pre_pre_dx_count, 0) as pre_pre_dx_count, 
    nvl(pre_dx_count, 0) as pre_dx_count, 
    nvl(covid_dx_count, 0) as covid_dx_count,
    nvl(post_dx_count, 0) as post_dx_count
	FROM four_windows_dx_counts
	) tbl 
ON feat.person_id = tbl.person_id

@transform_pandas(
    Output(rid="ri.vector.main.execute.052c1047-92d1-4c5a-96b5-c39efb023e6e"),
    Feature_table_builder=Input(rid="ri.foundry.main.dataset.ce7a93a0-4140-4fdb-b97d-fb78c0caf345"),
    condition_occurrence=Input(rid="ri.foundry.main.dataset.2f496793-6a4e-4bf4-b0fc-596b277fb7e2")
)
-- find all conditions associated with patients in their pre-pre window
SELECT feat.*, co.condition_concept_name, co.condition_concept_id, co.condition_start_date, co.condition_source_value, co.visit_occurrence_id
FROM Feature_table_builder feat 
JOIN 
condition_occurrence co ON feat.person_id = co.person_id and co.condition_start_date between feat.pre_pre_window_start_dt and feat.pre_window_start_dt

