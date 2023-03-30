

@transform_pandas(
    Output(rid="ri.vector.main.execute.a63a8ea7-8537-4ef5-9e68-274c3b5bb545"),
    concept=Input(rid="ri.foundry.main.dataset.5cb3c4a3-327a-47bf-a8bf-daf0cafe6772")
)
SELECT *
FROM concept
where domain_id = 'Drug' and lower(vocabulary_id) = 'rxnorm' and concept_class_id = 'Ingredient' and standard_concept = 'S'

@transform_pandas(
    Output(rid="ri.vector.main.execute.288cdbd1-0d61-4a65-a86d-1714402f663f"),
    Feature_table_builder=Input(rid="ri.foundry.main.dataset.ce7a93a0-4140-4fdb-b97d-fb78c0caf345"),
    drug_exposure=Input(rid="ri.foundry.main.dataset.469b3181-6336-4d0e-8c11-5e33a99876b5")
)
-- another performance assist; this subsets the giant drug_exposure table just to those drugs that are associated with a patient in our cohort
SELECT d.*
FROM drug_exposure d JOIN Feature_table_builder f ON d.person_id = f.person_id

@transform_pandas(
    Output(rid="ri.vector.main.execute.130284d0-8168-4ecb-8556-9646aa90cd07"),
    Feature_table_builder=Input(rid="ri.foundry.main.dataset.ce7a93a0-4140-4fdb-b97d-fb78c0caf345"),
    drugRollUp=Input(rid="ri.vector.main.execute.685e760b-462c-47c5-b2b2-5e8d9b1b4973")
)
-- pull all the drugs associated with the patient in their pre window
SELECT feat.*, co.ancestor_drug_concept_name, co.ancestor_drug_concept_id, co.drug_exposure_start_date, co.visit_occurrence_id
FROM Feature_table_builder feat JOIN drugRollUp co ON feat.person_id = co.person_id and co.drug_exposure_start_date between feat.pre_window_end_dt and feat.post_window_start_dt

@transform_pandas(
    Output(rid="ri.vector.main.execute.99e86a2a-69e5-4c25-b6e8-6ea4ade87b3f"),
    Feature_table_builder=Input(rid="ri.foundry.main.dataset.ce7a93a0-4140-4fdb-b97d-fb78c0caf345"),
    covid_drugs=Input(rid="ri.vector.main.execute.130284d0-8168-4ecb-8556-9646aa90cd07"),
    microvisits_to_macrovisits=Input(rid="ri.foundry.main.dataset.d77a701f-34df-48a1-a71c-b28112a07ffa")
)
SELECT feat.person_id, feat.patient_group, prc.ancestor_drug_concept_name, prc.ancestor_drug_concept_id, 'covid count' as count_type, count(distinct nvl(mml.macrovisit_id, mml.visit_occurrence_id)) as med_count
    FROM Feature_table_builder feat
            JOIN microvisits_to_macrovisits mml on feat.person_id = mml.person_id
            JOIN covid_drugs prc on mml.visit_occurrence_id = prc.visit_occurrence_id
    GROUP BY 
	feat.person_id, feat.patient_group, prc.ancestor_drug_concept_name, prc.ancestor_drug_concept_id

@transform_pandas(
    Output(rid="ri.vector.main.execute.685e760b-462c-47c5-b2b2-5e8d9b1b4973"),
    DrugConcepts=Input(rid="ri.vector.main.execute.a63a8ea7-8537-4ef5-9e68-274c3b5bb545"),
    Drugs_for_These_Patients=Input(rid="ri.vector.main.execute.288cdbd1-0d61-4a65-a86d-1714402f663f"),
    concept_ancestor=Input(rid="ri.foundry.main.dataset.c5e0521a-147e-4608-b71e-8f53bcdbe03c")
)
-- roll up all drugs to RxNorm ingredient level for consistency
SELECT distinct ds.person_id, ds.drug_exposure_start_date, ds.visit_occurrence_id, ds.drug_concept_id as original_drug_concept_id, ds.drug_concept_name as original_drug_concept_name, dc.concept_id as ancestor_drug_concept_id, dc.concept_name as ancestor_drug_concept_name
-- sing only the portion of concept_ancestor where the ancestors are rxnorm ingredients and are standard concepts.
FROM DrugConcepts dc JOIN concept_ancestor ca ON dc.concept_id = ca.ancestor_concept_id -- the ingredients are the ancestors
-- if a med for one of our patients is a descendent of one of those ingredients
    JOIN Drugs_for_These_Patients ds ON ds.drug_concept_id = ca.descendant_concept_id -- the original meds are the descendents 

@transform_pandas(
    Output(rid="ri.vector.main.execute.d7cd5658-cbb1-418e-9b84-a8777aa67f19"),
    Feature_table_builder=Input(rid="ri.foundry.main.dataset.ce7a93a0-4140-4fdb-b97d-fb78c0caf345"),
    drugRollUp=Input(rid="ri.vector.main.execute.685e760b-462c-47c5-b2b2-5e8d9b1b4973")
)
-- pull all the drugs associated with the patient in their post window
SELECT feat.*, co.ancestor_drug_concept_name, co.ancestor_drug_concept_id, co.drug_exposure_start_date, co.visit_occurrence_id
FROM Feature_table_builder feat JOIN drugRollUp co ON feat.person_id = co.person_id and co.drug_exposure_start_date between feat.post_window_start_dt and feat.post_window_end_dt

@transform_pandas(
    Output(rid="ri.vector.main.execute.f7b478eb-85f4-43a0-949c-2ecc78140e17"),
    Feature_table_builder=Input(rid="ri.foundry.main.dataset.ce7a93a0-4140-4fdb-b97d-fb78c0caf345"),
    microvisits_to_macrovisits=Input(rid="ri.foundry.main.dataset.d77a701f-34df-48a1-a71c-b28112a07ffa"),
    post_drugs=Input(rid="ri.vector.main.execute.d7cd5658-cbb1-418e-9b84-a8777aa67f19")
)
SELECT feat.person_id, feat.patient_group, prc.ancestor_drug_concept_name, prc.ancestor_drug_concept_id, 'post count' as count_type, count(distinct nvl(mml.macrovisit_id, mml.visit_occurrence_id)) as med_count
	FROM Feature_table_builder feat 
    		JOIN microvisits_to_macrovisits mml ON feat.person_id = mml.person_id
    		JOIN post_drugs prc ON mml.visit_occurrence_id = prc.visit_occurrence_id
	GROUP BY 
	feat.person_id, feat.patient_group, prc.ancestor_drug_concept_name, prc.ancestor_drug_concept_id

@transform_pandas(
    Output(rid="ri.vector.main.execute.6420351f-3985-4ec5-b098-66c21eb6900a"),
    Feature_table_builder=Input(rid="ri.foundry.main.dataset.ce7a93a0-4140-4fdb-b97d-fb78c0caf345"),
    drugRollUp=Input(rid="ri.vector.main.execute.685e760b-462c-47c5-b2b2-5e8d9b1b4973")
)
-- pull all the drugs associated with the patient in their pre window
SELECT feat.*, co.ancestor_drug_concept_name, co.ancestor_drug_concept_id, co.drug_exposure_start_date, co.visit_occurrence_id
FROM Feature_table_builder feat JOIN drugRollUp co ON feat.person_id = co.person_id and co.drug_exposure_start_date between feat.pre_window_start_dt and feat.pre_window_end_dt

@transform_pandas(
    Output(rid="ri.vector.main.execute.3bd5ba7c-d0c2-4485-9d10-c16895794ea0"),
    covidtbl=Input(rid="ri.vector.main.execute.99e86a2a-69e5-4c25-b6e8-6ea4ade87b3f"),
    posttbl=Input(rid="ri.vector.main.execute.f7b478eb-85f4-43a0-949c-2ecc78140e17"),
    prepretbl=Input(rid="ri.vector.main.execute.753f92de-1931-408e-ade4-9d18a7f4bb76"),
    pretbl=Input(rid="ri.vector.main.execute.0c161a09-22c1-421b-b6ef-df510fa5d02c")
)
-- do a full outer join between pre and post drugs so as to compare

SELECT 
prepretbl.person_id as prepre_person_id, prepretbl.patient_group as prepre_patient_group, prepretbl.ancestor_drug_concept_name as prepre_ancestor_drug_concept_name, prepretbl.ancestor_drug_concept_id as prepre_ancestor_drug_concept_id, prepretbl.count_type as prepre_count_type, prepretbl.med_count as prepre_med_count,

pretbl.person_id as pre_person_id, pretbl.patient_group as pre_patient_group, pretbl.ancestor_drug_concept_name as pre_ancestor_drug_concept_name, pretbl.ancestor_drug_concept_id as pre_ancestor_drug_concept_id, pretbl.count_type as pre_count_type, pretbl.med_count as pre_med_count,

covidtbl.person_id as covid_person_id, covidtbl.patient_group as covid_patient_group, covidtbl.ancestor_drug_concept_name as covid_ancestor_drug_concept_name, covidtbl.ancestor_drug_concept_id as covid_ancestor_drug_concept_id, covidtbl.count_type as covid_count_type, covidtbl.med_count as covid_med_count,

posttbl.person_id as post_person_id, posttbl.patient_group as post_patient_group, posttbl.ancestor_drug_concept_name as post_ancestor_drug_concept_name, posttbl.ancestor_drug_concept_id as post_ancestor_drug_concept_id, posttbl.count_type as post_count_type, posttbl.med_count as post_med_count
FROM 
    -- pre pre drug
    prepretbl
    FULL JOIN
    
    -- pre drug table 
    pretbl on pretbl.person_id = prepretbl.person_id  AND pretbl.ancestor_drug_concept_name = prepretbl.ancestor_drug_concept_name

    FULL JOIN
    -- covid drug table
    covidtbl on pretbl.person_id = covidtbl.person_id  AND pretbl.ancestor_drug_concept_name = covidtbl.ancestor_drug_concept_name
    
	FULL JOIN
    -- post drug table
    posttbl
	ON  covidtbl.person_id = posttbl.person_id  AND covidtbl.ancestor_drug_concept_name = posttbl.ancestor_drug_concept_name

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.fa3fe17e-58ac-4615-a238-0fc24ecd9b6e"),
    Feature_table_builder=Input(rid="ri.foundry.main.dataset.ce7a93a0-4140-4fdb-b97d-fb78c0caf345"),
    pre_post_med_count=Input(rid="ri.vector.main.execute.3bd5ba7c-d0c2-4485-9d10-c16895794ea0")
)
-- clean up the full outer join for meds. this table will be handed off as is to the next part of the pipeline
-- STOP: The results of the query below need to be stored in a table called "pre_post_med_count_clean". Once you have this table created, move to the next script in the sequence.
select distinct tbl.*, feat.apprx_age, feat.sex, feat.race, feat.ethn, feat.tot_long_data_days, feat.op_post_visit_ratio as op_post_visit_ratio, feat.post_ip_visit_ratio, feat.covid_ip_visit_ratio, feat.post_icu_visit_ratio, feat.covid_icu_visit_ratio
FROM Feature_table_builder feat 
JOIN
(SELECT 
nvl(nvl(nvl(prepre_person_id, pre_person_id), covid_person_id), post_person_id) as person_id, 
nvl(nvl(nvl(prepre_patient_group, pre_patient_group), covid_patient_group), post_patient_group) as patient_group, 
nvl(nvl(nvl(prepre_ancestor_drug_concept_name, pre_ancestor_drug_concept_name), covid_ancestor_drug_concept_name), post_ancestor_drug_concept_name) as ancestor_drug_concept_name, 
nvl(nvl(nvl(prepre_ancestor_drug_concept_id, pre_ancestor_drug_concept_id), covid_ancestor_drug_concept_id), post_ancestor_drug_concept_id) as ancestor_drug_concept_id, 
nvl(prepre_med_count, 0) as pre_pre_med_count, 
nvl(pre_med_count, 0) as pre_med_count, 
nvl(covid_med_count, 0) as covid_med_count, 
nvl(post_med_count, 0) as post_med_count
FROM pre_post_med_count
) tbl 
ON feat.person_id = tbl.person_id

@transform_pandas(
    Output(rid="ri.vector.main.execute.9b9a05ee-943a-4764-9585-7f94c813af83"),
    Feature_table_builder=Input(rid="ri.foundry.main.dataset.ce7a93a0-4140-4fdb-b97d-fb78c0caf345"),
    drugRollUp=Input(rid="ri.vector.main.execute.685e760b-462c-47c5-b2b2-5e8d9b1b4973")
)
-- pull all the drugs associated with the patient in their pre window
SELECT feat.*, co.ancestor_drug_concept_name, co.ancestor_drug_concept_id, co.drug_exposure_start_date, co.visit_occurrence_id
FROM Feature_table_builder feat JOIN drugRollUp co ON feat.person_id = co.person_id and co.drug_exposure_start_date between feat.pre_pre_window_start_dt and feat.pre_window_start_dt

@transform_pandas(
    Output(rid="ri.vector.main.execute.753f92de-1931-408e-ade4-9d18a7f4bb76"),
    Feature_table_builder=Input(rid="ri.foundry.main.dataset.ce7a93a0-4140-4fdb-b97d-fb78c0caf345"),
    microvisits_to_macrovisits=Input(rid="ri.foundry.main.dataset.d77a701f-34df-48a1-a71c-b28112a07ffa"),
    pre_pre_drugs=Input(rid="ri.vector.main.execute.9b9a05ee-943a-4764-9585-7f94c813af83")
)
SELECT feat.person_id, feat.patient_group, prc.ancestor_drug_concept_name, prc.ancestor_drug_concept_id, 'pre pre count' as count_type, count(distinct nvl(mml.macrovisit_id, mml.visit_occurrence_id)) as med_count
    FROM Feature_table_builder feat
            JOIN microvisits_to_macrovisits mml on feat.person_id = mml.person_id
            JOIN pre_pre_drugs prc on mml.visit_occurrence_id = prc.visit_occurrence_id
    GROUP BY 
	feat.person_id, feat.patient_group, prc.ancestor_drug_concept_name, prc.ancestor_drug_concept_id

@transform_pandas(
    Output(rid="ri.vector.main.execute.0c161a09-22c1-421b-b6ef-df510fa5d02c"),
    Feature_table_builder=Input(rid="ri.foundry.main.dataset.ce7a93a0-4140-4fdb-b97d-fb78c0caf345"),
    microvisits_to_macrovisits=Input(rid="ri.foundry.main.dataset.d77a701f-34df-48a1-a71c-b28112a07ffa"),
    pre_drugs=Input(rid="ri.vector.main.execute.6420351f-3985-4ec5-b098-66c21eb6900a")
)
SELECT feat.person_id, feat.patient_group, prc.ancestor_drug_concept_name, prc.ancestor_drug_concept_id, 'pre count' as count_type, count(distinct nvl(mml.macrovisit_id, mml.visit_occurrence_id)) as med_count
	FROM Feature_table_builder feat 
    		JOIN microvisits_to_macrovisits mml ON feat.person_id = mml.person_id
    		JOIN pre_drugs prc ON mml.visit_occurrence_id = prc.visit_occurrence_id
	GROUP BY 
	feat.person_id, feat.patient_group, prc.ancestor_drug_concept_name, prc.ancestor_drug_concept_id

