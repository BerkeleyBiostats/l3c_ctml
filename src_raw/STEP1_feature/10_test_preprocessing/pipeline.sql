

@transform_pandas(
    Output(rid="ri.vector.main.execute.2fef4d8c-9e31-463a-b919-d9ea3e2f78b9"),
    Long_COVID_Silver_Standard_Blinded=Input(rid="ri.foundry.main.dataset.cb65632b-bdff-4aa9-8696-91bc6667e2ba"),
    manifest_safe_harbor=Input(rid="ri.foundry.main.dataset.7a5c5585-1c69-4bf5-9757-3fd0d0a209a2"),
    person=Input(rid="ri.foundry.main.dataset.06629068-25fc-4802-9b31-ead4ed515da4")
)
-- determine earliest index date, filter base population on amount of post-covid data available, add demographic variables
-- this is a query you will need to adjust, join-wise, if you have not pre-joined the concept table to your person table.
SELECT distinct 
    pr.person_id, 
    (year(msh.run_date) - pr.year_of_birth) as apprx_age,	-- SITE_INFO: Uses most recent site data update date as anchor for age calculation
    pr.gender_concept_name as sex, 
    pr.race_concept_name as race, 
    pr.ethnicity_concept_name as ethn, 
    pr.data_partner_id as site_id,                              -- SITE_INFO: Pulls in site ID for each patient row; useful for troubleshooting later
    covid_index as min_covid_dt
FROM Long_COVID_Silver_Standard_Blinded vc 
    JOIN person pr ON vc.person_id = pr.person_id
    LEFT JOIN manifest_safe_harbor msh ON pr.data_partner_id = msh.data_partner_id 	-- SITE_INFO: Joining to the manifest table here
-- WHERE
--     datediff(msh.run_date, array_min(array(covid_index, d.death_date))) >= 100

@transform_pandas(
    Output(rid="ri.vector.main.execute.60346ff3-e886-4714-9402-81b6c2e328ba"),
    concept=Input(rid="ri.foundry.main.dataset.5cb3c4a3-327a-47bf-a8bf-daf0cafe6772")
)
SELECT *
FROM concept
where domain_id = 'Drug' and lower(vocabulary_id) = 'rxnorm' and concept_class_id = 'Ingredient' and standard_concept = 'S'

@transform_pandas(
    Output(rid="ri.vector.main.execute.16e9328b-7dfa-48d8-b60b-ea5b945ea683"),
    Feature_table_builder=Input(rid="ri.foundry.main.dataset.e0264d38-57aa-4509-9245-3d008b0526c4"),
    drug_exposure=Input(rid="ri.foundry.main.dataset.26a51cab-0279-45a6-bbc0-f44a12b52f9c")
)
-- another performance assist; this subsets the giant drug_exposure table just to those drugs that are associated with a patient in our cohort
SELECT d.*
FROM drug_exposure d JOIN Feature_table_builder f ON d.person_id = f.person_id

@transform_pandas(
    Output(rid="ri.vector.main.execute.e167e968-4f04-4205-8339-0336930dd83f"),
    Long_COVID_Silver_Standard_Blinded=Input(rid="ri.foundry.main.dataset.cb65632b-bdff-4aa9-8696-91bc6667e2ba"),
    hosp_and_non=Input(rid="ri.vector.main.execute.d357092e-6841-48fb-8d5f-809b4a1ae4fb"),
    manifest_safe_harbor=Input(rid="ri.foundry.main.dataset.7a5c5585-1c69-4bf5-9757-3fd0d0a209a2"),
    microvisits_to_macrovisits=Input(rid="ri.foundry.main.dataset.f5008fa4-e736-4244-88e1-1da7a68efcdb")
)
/* 
Shareable N3C PASC Phenotype code
V1.0, last updated May 2022 by Emily Pfaff
PLEASE READ THE NOTES BELOW AND THE COMMENTS ABOVE EACH CTE BEFORE EXECUTING
Assumptions:
-- You have built out a "macrovisit" table. (Code to create this table is included in this GitHub repository.) In this script, the macrovisit table is called "microvisit_to_macrovisit_lds". If you use a different name, you will need to CTRL+F for this string and replace with your table name.
-- You are receiving data from multiple sites, and (1) have some mechanism to identify which rows of data come from which site, and (2) have a table that stores the most recent date that each site's data was updated. We call this table our "manifest" table; if you use a different table name, or column names, you may need to make some edits. I have marked all instances where this our site ID variable or the manifest table appears with "-- SITE_INFO". You can find all instances in the code if you CTRL+F for that string.
Notes:
-- This is written in Spark SQL, which is mostly similar to other SQL dialects, but certainly differs in syntax for particular functions. One that I already know will be an issue is the date_add function, and most likely the datediff function. You will need to CTRL+F for each problematic function and replace each one with the equivalent from your RDBNMS.
-- For efficiency, we have pre-joined the OMOP CONCEPT table to all of the core OMOP tables using all possible foreign keys. Thus, you will see that we, e.g., select "race_concept_name" directly from the PERSON table, when that would usually require a join to CONCEPT on race_concept_id to get the name. If you do not wish to pre-join in your own repository, you will need to add in one or more JOINs to one or more aliases of the CONCEPT table in all areas where this occurs. 
-- I have written this script as a series of CTEs. This is likely not performant--it will almost certainly be better to stop at certain points in this flow and create materialized tables so you don't have to hold everything in memory. I have not done this ahead of time because every RDBMS handles this differently. 
*/
/*
SELECT * FROM 
	(
 */   -- calcualte percentage of visits
	SELECT ottbl.*, nvl(count(distinct visit_start_date), 0) as post_visits_count, datediff(ottbl.post_window_end_dt,ottbl.post_window_start_dt) as tot_post_days, datediff(ottbl.post_window_start_dt,ottbl.pre_window_end_dt) as tot_covid_days, nvl(count(distinct visit_start_date),0)/14 as op_post_visit_ratio
	FROM 
		(
        -- calculate pre-covid, post-covid windows
		SELECT distinct 
		hc.*, 
		datediff(max(mm.visit_start_date),min(mm.visit_start_date)) as tot_long_data_days, 
		date_add(hc.min_covid_dt,-365) as pre_pre_window_start_dt, 
        date_add(hc.min_covid_dt,-37) as pre_window_start_dt, 
		date_add(hc.min_covid_dt, -7) as pre_window_end_dt,
		date_add(hc.min_covid_dt, 14) as post_window_start_dt, 
        date_add(hc.min_covid_dt, 28) as post_window_end_dt
		--case when date_add(hc.min_covid_dt, 300) < msh.run_date 
			--then date_add(hc.min_covid_dt, 300) 
			--else msh.run_date end as post_window_end_dt 			-- SITE_INFO: Using the most recent site data update date to determine number of longitudinal patient days
		FROM hosp_and_non hc LEFT JOIN manifest_safe_harbor msh ON hc.site_id = msh.data_partner_id            -- SITE_INFO: Pulls in site ID for each patient row; useful for troubleshooting later
    			LEFT JOIN microvisits_to_macrovisits mm ON hc.person_id = mm.person_id
    			JOIN Long_COVID_Silver_Standard_Blinded lc ON hc.person_id = lc.person_id
		GROUP BY
		hc.person_id, hc.patient_group, hc.apprx_age, hc.sex, hc.race, hc.ethn, hc.site_id, hc.min_covid_dt, msh.run_date --, lc.pasc_index
		) 
        ottbl
	LEFT JOIN microvisits_to_macrovisits mmpost ON (ottbl.person_id = mmpost.person_id and mmpost.visit_start_date between ottbl.post_window_start_dt and 
        	ottbl.post_window_end_dt and mmpost.macrovisit_id is null)
	-- WHERE tot_long_data_days > 0 
	GROUP BY ottbl.person_id, ottbl.patient_group, ottbl.apprx_age, ottbl.sex, ottbl.race, ottbl.ethn, ottbl.site_id, ottbl.min_covid_dt, ottbl.tot_long_data_days, ottbl.pre_pre_window_start_dt, ottbl.pre_window_start_dt, ottbl.pre_window_end_dt, 	
	ottbl.post_window_start_dt, ottbl.post_window_end_dt
/*	) pts
WHERE post_visits_count >=1
*/

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.e0264d38-57aa-4509-9245-3d008b0526c4"),
    Feature_Table_Builder_v0=Input(rid="ri.vector.main.execute.e167e968-4f04-4205-8339-0336930dd83f"),
    tot_icu_days_calc=Input(rid="ri.vector.main.execute.fde6567c-4e53-42e9-b07a-d733fb443471"),
    tot_ip_days_calc=Input(rid="ri.vector.main.execute.692f0334-6976-4043-9038-27e2d8836f79")
)
SELECT feat.*, 
(nvl(tot_ip.post_tot_ip_days, 0)/feat.tot_post_days) as post_ip_visit_ratio, 
(nvl(tot_ip.covid_tot_ip_days, 0)/feat.tot_covid_days) as covid_ip_visit_ratio, 
(nvl(tot_icu.post_tot_icu_days, 0)/feat.tot_post_days) as post_icu_visit_ratio, 
(nvl(tot_icu.covid_tot_icu_days, 0)/feat.tot_covid_days) as covid_icu_visit_ratio
FROM Feature_Table_Builder_v0 feat
LEFT JOIN tot_ip_days_calc tot_ip ON feat.person_id = tot_ip.person_id
LEFT JOIN tot_icu_days_calc tot_icu ON feat.person_id = tot_icu.person_id

@transform_pandas(
    Output(rid="ri.vector.main.execute.02850990-abf4-4b63-82b7-dbad6fda7ed7"),
    Collect_the_Cohort=Input(rid="ri.vector.main.execute.2fef4d8c-9e31-463a-b919-d9ea3e2f78b9"),
    condition_occurrence=Input(rid="ri.foundry.main.dataset.3e01546f-f110-4c67-a6db-9063d2939a74"),
    microvisits_to_macrovisits=Input(rid="ri.foundry.main.dataset.f5008fa4-e736-4244-88e1-1da7a68efcdb")
)
-- hospitalization close to index date
SELECT b.person_id, b.apprx_age, b.sex, b.race, b.ethn, site_id, min_covid_dt
FROM Collect_the_Cohort b JOIN microvisits_to_macrovisits mac ON b.person_id = mac.person_id
WHERE
    mac.macrovisit_start_date between date_add(b.min_covid_dt, -14) and date_add(b.min_covid_dt, 14)

UNION

-- hospitalization with a U07.1
SELECT b.person_id, b.apprx_age, b.sex, b.race, b.ethn, site_id, min_covid_dt
FROM Collect_the_Cohort b JOIN microvisits_to_macrovisits mac ON b.person_id = mac.person_id
    JOIN condition_occurrence cond ON mac.visit_occurrence_id = cond.visit_occurrence_id
WHERE
    mac.macrovisit_id is not null
    and condition_concept_id = 37311061 

@transform_pandas(
    Output(rid="ri.vector.main.execute.192a0bb9-7930-4f8a-8227-405ff5d0f6a0"),
    microvisits_to_macrovisits=Input(rid="ri.foundry.main.dataset.f5008fa4-e736-4244-88e1-1da7a68efcdb")
)
SELECT distinct 
    person_id, visit_concept_name, macrovisit_id, macrovisit_start_date, macrovisit_end_date
        FROM microvisits_to_macrovisits
        WHERE visit_concept_name IN ( 'Inpatient Critical Care Facility', 
        'Emergency Room and Inpatient Visit', 
        'Emergency Room Visit', 'Intensive Care', 'Emergency Room - Hospital')
        and macrovisit_id is not null

@transform_pandas(
    Output(rid="ri.vector.main.execute.1b318b0b-fa44-4315-90c7-509b4bedc06b"),
    Feature_table_builder=Input(rid="ri.foundry.main.dataset.e0264d38-57aa-4509-9245-3d008b0526c4"),
    high_level_condition_occur=Input(rid="ri.vector.main.execute.e7aa7bae-473d-44bb-8869-9fa53186e2f7")
)
SELECT person_id, nvl(prediabetes, 0) as prediabetes,  nvl(nvl(diabetes, diabetes_complications), 0) as diabetes, nvl(chronic_kidney_disease, 0) as chronic_kidney_disease, nvl(congestive_heart_failure, 0) as congestive_heart_failure, nvl(chronic_pulmonary_disease, 0) as chronic_pulmonary_disease
FROM
(
    SELECT feat.person_id, feat.patient_group, hlc.high_level_condition, count(distinct hlc.high_level_condition) as comorbidity_count
	FROM Feature_table_builder feat 
    		LEFT JOIN high_level_condition_occur hlc ON feat.person_id = hlc.person_id
	GROUP BY 
	feat.person_id, feat.patient_group, hlc.high_level_condition
)
PIVOT (
    SUM(comorbidity_count)
    FOR high_level_condition IN (
        'diabetes', 'prediabetes', 'diabetes_complications', 'chronic_kidney_disease','congestive_heart_failure', 'chronic_pulmonary_disease'
    )
);
 
	

@transform_pandas(
    Output(rid="ri.vector.main.execute.b8332970-286e-4e80-ac5f-61e50b0ba286"),
    Feature_table_builder=Input(rid="ri.foundry.main.dataset.e0264d38-57aa-4509-9245-3d008b0526c4"),
    condition_occurrence=Input(rid="ri.foundry.main.dataset.3e01546f-f110-4c67-a6db-9063d2939a74")
)
-- find all conditions associated with patients in their acute covid window
SELECT feat.*, co.condition_concept_name, co.condition_concept_id, co.condition_start_date, co.condition_source_value, co.visit_occurrence_id
FROM Feature_table_builder feat 
JOIN 
condition_occurrence co ON feat.person_id = co.person_id and co.condition_start_date between feat.pre_window_end_dt and feat.post_window_start_dt

@transform_pandas(
    Output(rid="ri.vector.main.execute.e128cac3-b76d-430a-8c8e-ed2b0a9eae2a"),
    Feature_table_builder=Input(rid="ri.foundry.main.dataset.e0264d38-57aa-4509-9245-3d008b0526c4"),
    drugRollUp=Input(rid="ri.vector.main.execute.e79d57fa-0fc2-4e7a-9940-603568a40ae9")
)
-- pull all the drugs associated with the patient in their pre window
SELECT feat.*, co.ancestor_drug_concept_name, co.ancestor_drug_concept_id, co.drug_exposure_start_date, co.visit_occurrence_id
FROM Feature_table_builder feat JOIN drugRollUp co ON feat.person_id = co.person_id and co.drug_exposure_start_date between feat.pre_window_end_dt and feat.post_window_start_dt

@transform_pandas(
    Output(rid="ri.vector.main.execute.ced6c707-79e8-416f-81f5-7c7f2a0548f6"),
    covid_person=Input(rid="ri.foundry.main.dataset.e5aa8575-e263-4cd1-97cd-a768cb7f975c")
)
SELECT nvl(covidtbl.person_id, posttbl.person_id) as person_id, nvl(covidtbl.measure_type, posttbl.measure_type) as measure_type, covidtbl.c_any_pos, covidtbl.c_any_measure, posttbl.post_any_pos, posttbl.post_any_measure

FROM
-- acute covid window
    (SELECT distinct c.person_id, c.measure_type, max(c.pos) as c_any_pos, 1 as c_any_measure
    FROM
        (SELECT cp.person_id, cp.measurement_concept_name as measure_type, pos_or_neg as pos
        FROM covid_person cp
        WHERE cp.measurement_date > cp.pre_window_end_dt and cp.measurement_date <= cp.post_window_start_dt
        ) as c 
    GROUP BY c.person_id, c.measure_type) covidtbl

FULL JOIN 

-- post covid window
    (SELECT distinct c.person_id, c.measure_type, max(c.pos) as post_any_pos, 1 as post_any_measure
    FROM
        (SELECT cp.person_id, cp.measurement_concept_name as measure_type, pos_or_neg as pos
        FROM covid_person cp
        WHERE cp.measurement_date > cp.post_window_start_dt and cp.measurement_date <= cp.post_window_end_dt
        ) as c 
    GROUP BY c.person_id, c.measure_type) posttbl

ON covidtbl.person_id = posttbl.person_id AND covidtbl.measure_type = posttbl.measure_type

@transform_pandas(
    Output(rid="ri.vector.main.execute.ee965a9d-9d2f-4175-aba8-3c842882c7cb"),
    Feature_table_builder=Input(rid="ri.foundry.main.dataset.e0264d38-57aa-4509-9245-3d008b0526c4"),
    measurement_person=Input(rid="ri.vector.main.execute.b58177f1-cd68-4be9-be4b-715f899c2a0b")
)
-- find all conditions associated with patients in their pre-pre window
SELECT feat.person_id, m.measurement_concept_name, m.measurement_concept_id, 
max(m.harmonized_value_as_number) as max_measure, min(m.harmonized_value_as_number) as min_measure, 
avg(m.harmonized_value_as_number) as avg_measure
FROM Feature_table_builder feat 
JOIN 
measurement_person m ON feat.person_id = m.person_id and m.visit_date between feat.pre_window_end_dt and feat.post_window_start_dt
GROUP BY feat.person_id, m.measurement_concept_name, m.measurement_concept_id

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.00f71213-03c7-44f8-be40-520411e33272"),
    covid_measure_indicators=Input(rid="ri.vector.main.execute.ced6c707-79e8-416f-81f5-7c7f2a0548f6"),
    start_end_date=Input(rid="ri.vector.main.execute.24643c26-4692-4225-92be-b81a187ae80e")
)
SELECT nvl(cmi.person_id, sed.person_id) as person_id, nvl(cmi.measure_type, sed.measure_type) as measure_type, 
        cmi.c_any_measure, cmi.c_any_pos, sed.c_covid_length, sed.c_impute_covid_length,
        cmi.post_any_measure, cmi.post_any_pos, sed.post_covid_length, sed.post_impute_covid_length

FROM 

    covid_measure_indicators cmi 

    FULL JOIN 

    (SELECT nvl(covidtbl.person_id, posttbl.person_id) as person_id, nvl(covidtbl.measure_type, posttbl.measure_type) as measure_type, 
            covidtbl.c_covid_length, covidtbl.c_impute_covid_length, posttbl.post_covid_length, posttbl.post_impute_covid_length

    FROM
        (SELECT distinct person_id, measure_type, covid_length as c_covid_length, impute_covid_length as c_impute_covid_length
        FROM start_end_date sed
        WHERE sed.window_type == "covid") covidtbl

        FULL JOIN

        (SELECT distinct person_id, measure_type, covid_length as post_covid_length, impute_covid_length as post_impute_covid_length
        FROM start_end_date sed
        WHERE sed.window_type == "pos_covid") posttbl

        ON covidtbl.person_id = posttbl.person_id AND covidtbl.measure_type = posttbl.measure_type
    ) sed 

ON cmi.person_id = sed.person_id AND cmi.measure_type = sed.measure_type

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.e5aa8575-e263-4cd1-97cd-a768cb7f975c"),
    Feature_table_builder=Input(rid="ri.foundry.main.dataset.e0264d38-57aa-4509-9245-3d008b0526c4"),
    measurement=Input(rid="ri.foundry.main.dataset.b7749e49-cf01-4d0a-a154-2f00eecab21e")
)
SELECT ft.person_id, max(ft.pre_window_end_dt) as pre_window_end_dt, max(ft.post_window_start_dt) as post_window_start_dt, max(ft.post_window_end_dt) as post_window_end_dt, 
    m.measurement_date, m.measurement_concept_name,
    max(CASE WHEN m.value_as_concept_name IN ('Detected', 'Positive')
           THEN 1 
           ELSE 0
           END) as pos_or_neg
FROM Feature_table_builder ft
JOIN measurement m
on ft.person_id = m.person_id
WHERE m.value_as_concept_name IN ('Not detected', 'Detected', 'Positive', 'Negative')
AND m.measurement_date IS NOT NULL
AND m.measurement_concept_name IN ('SARS-CoV-2 (COVID-19) RNA [Presence] in Respiratory specimen by NAA with probe detection', 'SARS-CoV-2 (COVID-19) RNA [Presence] in Specimen by NAA with probe detection', 'SARS-CoV-2 (COVID-19) N gene [Presence] in Specimen by Nucleic acid amplification using CDC primer-probe set N1', 'SARS-CoV-2 (COVID-19) ORF1ab region [Presence] in Respiratory specimen by NAA with probe detection', 'SARS-CoV-2 (COVID-19) Ag [Presence] in Respiratory specimen by Rapid immunoassay', 'SARS-CoV-2 (COVID-19) RdRp gene [Presence] in Respiratory specimen by NAA with probe detection', 'SARS-CoV-2 (COVID-19) RdRp gene [Presence] in Specimen by NAA with probe detection', 'SARS-CoV+SARS-CoV-2 (COVID-19) Ag [Presence] in Respiratory specimen by Rapid immunoassay', 'SARS-CoV-2 (COVID-19) RNA panel - Specimen by NAA with probe detection', 'SARS-CoV-2 (COVID-19) RNA [Presence] in Nasopharynx by NAA with non-probe detection', 'SARS-CoV-2 (COVID-19) N gene [Presence] in Specimen by NAA with probe detection', 'SARS-CoV-2 (COVID-19) IgG Ab [Presence] in Serum or Plasma by Immunoassay')
GROUP BY ft.person_id, m.measurement_date, m.measurement_concept_name

@transform_pandas(
    Output(rid="ri.vector.main.execute.518c5020-2537-4606-a367-d0ec24710869"),
    covid_person=Input(rid="ri.foundry.main.dataset.e5aa8575-e263-4cd1-97cd-a768cb7f975c")
)
SELECT cp.person_id, cp.measurement_concept_name as measure_type,
    CASE WHEN cp.pos_or_neg = 1
    THEN cp.measurement_date
    ELSE NULL
    END as measure_pos_date,
    CASE WHEN cp.pos_or_neg = 0
    THEN cp.measurement_date
    ELSE NULL
    END as measure_neg_date
FROM covid_person cp
WHERE cp.measurement_date > cp.pre_window_end_dt and cp.measurement_date <= cp.post_window_start_dt

@transform_pandas(
    Output(rid="ri.vector.main.execute.dea2bc63-0ae2-4699-81d4-41fdf7f188b2"),
    Feature_table_builder=Input(rid="ri.foundry.main.dataset.e0264d38-57aa-4509-9245-3d008b0526c4"),
    covid_drugs=Input(rid="ri.vector.main.execute.e128cac3-b76d-430a-8c8e-ed2b0a9eae2a"),
    microvisits_to_macrovisits=Input(rid="ri.foundry.main.dataset.f5008fa4-e736-4244-88e1-1da7a68efcdb")
)
SELECT feat.person_id, feat.patient_group, prc.ancestor_drug_concept_name, prc.ancestor_drug_concept_id, 'covid count' as count_type, count(distinct nvl(mml.macrovisit_id, mml.visit_occurrence_id)) as med_count
    FROM Feature_table_builder feat
            JOIN microvisits_to_macrovisits mml on feat.person_id = mml.person_id
            JOIN covid_drugs prc on mml.visit_occurrence_id = prc.visit_occurrence_id
    GROUP BY 
	feat.person_id, feat.patient_group, prc.ancestor_drug_concept_name, prc.ancestor_drug_concept_id

@transform_pandas(
    Output(rid="ri.vector.main.execute.667ee0fd-96b1-41f0-a31a-384ba84c4354"),
    device_covid=Input(rid="ri.vector.main.execute.d9deca03-c0e0-4dd4-ac6c-2e5c778d8f66"),
    device_post=Input(rid="ri.vector.main.execute.80391b6d-de0d-433b-8f97-a8a57478b5da"),
    device_pre=Input(rid="ri.vector.main.execute.274b82d5-654e-4135-83ff-12c2ed782e73"),
    device_pre_pre=Input(rid="ri.vector.main.execute.1c02331a-1df2-49be-bbd9-c8f2abac6fdd")
)
-- do a full outer join between pre and post drugs so as to compare

SELECT 
prepretbl.person_id as prepre_person_id, prepretbl.device_concept_name as prepre_device_concept_name,   prepretbl.device_count as prepre_device_count,

pretbl.person_id as pre_person_id, pretbl.device_concept_name as pre_device_concept_name, pretbl.device_count as pre_device_count,

covidtbl.person_id as covid_person_id, covidtbl.device_concept_name as covid_device_concept_name, covidtbl.device_count as covid_device_count,

posttbl.person_id as post_person_id,  posttbl.device_concept_name as post_device_concept_name, posttbl.device_count as post_device_count
FROM 
    -- pre pre nlp
    device_pre_pre prepretbl
    
    FULL JOIN
    -- pre nlp
    device_pre pretbl on pretbl.person_id = prepretbl.person_id  AND pretbl.device_concept_name = prepretbl.device_concept_name

    FULL JOIN
    -- covid drug table
    device_covid covidtbl on pretbl.person_id = covidtbl.person_id  AND pretbl.device_concept_name = covidtbl.device_concept_name
    
	FULL JOIN
    -- post drug table
    device_post posttbl ON covidtbl.person_id = posttbl.person_id  AND covidtbl.device_concept_name = posttbl.device_concept_name

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.bfb42402-89ee-4333-9677-40d88ea630dd"),
    Feature_table_builder=Input(rid="ri.foundry.main.dataset.e0264d38-57aa-4509-9245-3d008b0526c4"),
    device_count=Input(rid="ri.vector.main.execute.667ee0fd-96b1-41f0-a31a-384ba84c4354")
)
select distinct tbl.*
FROM Feature_table_builder feat 
JOIN
(SELECT 
nvl(nvl(nvl(prepre_person_id, pre_person_id), covid_person_id), post_person_id) as person_id, 
nvl(nvl(nvl(prepre_device_concept_name, pre_device_concept_name), covid_device_concept_name), post_device_concept_name) as device_concept_name, 
nvl(prepre_device_count, 0) as pre_pre_device_count, 
nvl(pre_device_count, 0) as pre_device_count, 
nvl(covid_device_count, 0) as covid_device_count, 
nvl(post_device_count, 0) as post_device_count
FROM device_count
) tbl 
ON feat.person_id = tbl.person_id
WHERE tbl.device_concept_name is not null

@transform_pandas(
    Output(rid="ri.vector.main.execute.d9deca03-c0e0-4dd4-ac6c-2e5c778d8f66"),
    Feature_table_builder=Input(rid="ri.foundry.main.dataset.e0264d38-57aa-4509-9245-3d008b0526c4"),
    device_exposure=Input(rid="ri.foundry.main.dataset.7e24a101-2206-45d9-bcaa-b9d84bd2f990")
)
select d.person_id, d.device_concept_name, 1 as device_count
FROM
    (SELECT person_id, device_exposure_start_date, device_exposure_end_date, device_concept_name 
     FROM device_exposure 
     WHERE device_concept_name IN('Ventilator', 'Basic nasal oxygen cannula', 'High flow oxygen nasal cannula', 'N3C:Other oxygen device')) d
    JOIN 
    (SELECT person_id, pre_window_end_dt, post_window_start_dt FROM Feature_table_builder) feat
    ON d.person_id = feat.person_id 
       and ((d.device_exposure_start_date between feat.pre_window_end_dt and feat.post_window_start_dt) 
             or (d.device_exposure_end_date between feat.pre_window_end_dt and feat.post_window_start_dt) 
             or (d.device_exposure_start_date < feat.pre_window_end_dt and d.device_exposure_end_date > feat.post_window_start_dt))

@transform_pandas(
    Output(rid="ri.vector.main.execute.80391b6d-de0d-433b-8f97-a8a57478b5da"),
    Feature_table_builder=Input(rid="ri.foundry.main.dataset.e0264d38-57aa-4509-9245-3d008b0526c4"),
    device_exposure=Input(rid="ri.foundry.main.dataset.7e24a101-2206-45d9-bcaa-b9d84bd2f990")
)
select d.person_id, d.device_concept_name, 1 as device_count
FROM
    (SELECT person_id, device_exposure_start_date, device_exposure_end_date, device_concept_name 
     FROM device_exposure 
     WHERE device_concept_name IN('Ventilator', 'Basic nasal oxygen cannula', 'High flow oxygen nasal cannula', 'N3C:Other oxygen device')) d
    JOIN 
    (SELECT person_id, post_window_start_dt, post_window_end_dt FROM Feature_table_builder) feat
    ON d.person_id = feat.person_id 
       and ((d.device_exposure_start_date between feat.post_window_start_dt and feat.post_window_end_dt) 
             or (d.device_exposure_end_date between feat.post_window_start_dt and feat.post_window_end_dt) 
             or (d.device_exposure_start_date < feat.post_window_start_dt and d.device_exposure_end_date > feat.post_window_end_dt))

@transform_pandas(
    Output(rid="ri.vector.main.execute.274b82d5-654e-4135-83ff-12c2ed782e73"),
    Feature_table_builder=Input(rid="ri.foundry.main.dataset.e0264d38-57aa-4509-9245-3d008b0526c4"),
    device_exposure=Input(rid="ri.foundry.main.dataset.7e24a101-2206-45d9-bcaa-b9d84bd2f990")
)
select d.person_id, d.device_concept_name, 1 as device_count
FROM
    (SELECT person_id, device_exposure_start_date, device_exposure_end_date, device_concept_name 
     FROM device_exposure 
     WHERE device_concept_name IN('Ventilator', 'Basic nasal oxygen cannula', 'High flow oxygen nasal cannula', 'N3C:Other oxygen device')) d
    JOIN 
    (SELECT person_id, pre_window_start_dt, pre_window_end_dt FROM Feature_table_builder) feat
    ON d.person_id = feat.person_id 
       and ((d.device_exposure_start_date between feat.pre_window_start_dt and feat.pre_window_end_dt) 
             or (d.device_exposure_end_date between feat.pre_window_start_dt and feat.pre_window_end_dt) 
             or (d.device_exposure_start_date < feat.pre_window_start_dt and d.device_exposure_end_date > feat.pre_window_end_dt))

@transform_pandas(
    Output(rid="ri.vector.main.execute.1c02331a-1df2-49be-bbd9-c8f2abac6fdd"),
    Feature_table_builder=Input(rid="ri.foundry.main.dataset.e0264d38-57aa-4509-9245-3d008b0526c4"),
    device_exposure=Input(rid="ri.foundry.main.dataset.7e24a101-2206-45d9-bcaa-b9d84bd2f990")
)
select d.person_id, d.device_concept_name, 1 as device_count
FROM
    (SELECT person_id, device_exposure_start_date, device_exposure_end_date, device_concept_name 
     FROM device_exposure 
     WHERE device_concept_name IN('Ventilator', 'Basic nasal oxygen cannula', 'High flow oxygen nasal cannula', 'N3C:Other oxygen device')) d
    JOIN 
    (SELECT person_id, pre_pre_window_start_dt, pre_window_start_dt FROM Feature_table_builder) feat
    ON d.person_id = feat.person_id 
       and ((d.device_exposure_start_date between feat.pre_pre_window_start_dt and feat.pre_window_start_dt) 
             or (d.device_exposure_end_date between feat.pre_pre_window_start_dt and feat.pre_window_start_dt) 
             or (d.device_exposure_start_date < feat.pre_pre_window_start_dt and d.device_exposure_end_date > feat.pre_window_start_dt))

@transform_pandas(
    Output(rid="ri.vector.main.execute.e79d57fa-0fc2-4e7a-9940-603568a40ae9"),
    DrugConcepts=Input(rid="ri.vector.main.execute.60346ff3-e886-4714-9402-81b6c2e328ba"),
    Drugs_for_These_Patients=Input(rid="ri.vector.main.execute.16e9328b-7dfa-48d8-b60b-ea5b945ea683"),
    concept_ancestor=Input(rid="ri.foundry.main.dataset.c5e0521a-147e-4608-b71e-8f53bcdbe03c")
)
-- roll up all drugs to RxNorm ingredient level for consistency
SELECT distinct ds.person_id, ds.drug_exposure_start_date, ds.visit_occurrence_id, ds.drug_concept_id as original_drug_concept_id, ds.drug_concept_name as original_drug_concept_name, dc.concept_id as ancestor_drug_concept_id, dc.concept_name as ancestor_drug_concept_name
-- sing only the portion of concept_ancestor where the ancestors are rxnorm ingredients and are standard concepts.
FROM DrugConcepts dc JOIN concept_ancestor ca ON dc.concept_id = ca.ancestor_concept_id -- the ingredients are the ancestors
-- if a med for one of our patients is a descendent of one of those ingredients
    JOIN Drugs_for_These_Patients ds ON ds.drug_concept_id = ca.descendant_concept_id -- the original meds are the descendents 

@transform_pandas(
    Output(rid="ri.vector.main.execute.c65dde9f-1306-48e5-8bde-748eaec89f88"),
    Feature_table_builder=Input(rid="ri.foundry.main.dataset.e0264d38-57aa-4509-9245-3d008b0526c4"),
    covid_condition=Input(rid="ri.vector.main.execute.b8332970-286e-4e80-ac5f-61e50b0ba286"),
    microvisits_to_macrovisits=Input(rid="ri.foundry.main.dataset.f5008fa4-e736-4244-88e1-1da7a68efcdb"),
    post_condition=Input(rid="ri.vector.main.execute.0fe62ec5-fdaf-482e-aed0-df5e7ea4130d"),
    pre_condition=Input(rid="ri.vector.main.execute.7941d9bb-4846-4863-8b22-6013668847b4"),
    pre_pre_condition=Input(rid="ri.vector.main.execute.3e73242f-f520-4115-9631-bdbd0d73e6e0")
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
    Output(rid="ri.vector.main.execute.2b963a2e-59b2-480a-9d86-553513b25a89"),
    Feature_table_builder=Input(rid="ri.foundry.main.dataset.e0264d38-57aa-4509-9245-3d008b0526c4"),
    covid_measurement=Input(rid="ri.vector.main.execute.ee965a9d-9d2f-4175-aba8-3c842882c7cb"),
    post_measurement=Input(rid="ri.vector.main.execute.c247cc21-4fbc-419c-affe-bc7d560dd149"),
    pre_measurement=Input(rid="ri.vector.main.execute.09fc9cc5-5778-4c21-90a7-8b8550a63f10"),
    pre_pre_measurement=Input(rid="ri.vector.main.execute.c49ca582-65b0-49de-822d-55287579ddf8")
)
SELECT 
ppm.person_id as pre_pre_person_id, ppm.measurement_concept_name as pre_pre_measurement_concept_name, ppm.measurement_concept_id as pre_pre_measurement_concept_id,  
ppm.max_measure as pre_pre_max, ppm.min_measure as pre_pre_min, ppm.avg_measure as pre_pre_avg,

pm.person_id as pre_person_id, pm.measurement_concept_name as pre_measurement_concept_name, pm.measurement_concept_id as pre_measurement_concept_id, 
pm.max_measure as pre_max, pm.min_measure as pre_min, pm.avg_measure as pre_avg,

cm.person_id as covid_person_id, cm.measurement_concept_name as covid_measurement_concept_name, cm.measurement_concept_id as covid_measurement_concept_id, 
cm.max_measure as covid_max, cm.min_measure as covid_min, cm.avg_measure as covid_avg,

pom.person_id as post_person_id, pom.measurement_concept_name as post_measurement_concept_name, pom.measurement_concept_id as post_measurement_concept_id, 
pom.max_measure as post_max, pom.min_measure as post_min, pom.avg_measure as post_avg
FROM 
 pre_pre_measurement ppm

FULL JOIN
 pre_measurement pm
ON pm.person_id = ppm.person_id AND pm.measurement_concept_name = ppm.measurement_concept_name

FULL JOIN
 covid_measurement cm
ON pm.person_id = cm.person_id AND pm.measurement_concept_name = cm.measurement_concept_name

FULL JOIN
 post_measurement pom
ON pm.person_id = pom.person_id AND pm.measurement_concept_name = pom.measurement_concept_name

@transform_pandas(
    Output(rid="ri.vector.main.execute.e7aa7bae-473d-44bb-8869-9fa53186e2f7"),
    Feature_table_builder=Input(rid="ri.foundry.main.dataset.e0264d38-57aa-4509-9245-3d008b0526c4"),
    condition_occurrence=Input(rid="ri.foundry.main.dataset.3e01546f-f110-4c67-a6db-9063d2939a74")
)
-- find all high level conditions associated with patients in their pre window or pre-pre window
SELECT feat.person_id, feat.pre_window_start_dt, feat.pre_window_end_dt, glc.high_level_condition, glc.condition_start_date, glc.condition_end_date
FROM Feature_table_builder feat 
INNER JOIN (
    -- find all high level conditions associated with patients
    SELECT person_id, condition_occurrence_id, condition_end_date, condition_start_date, condition_concept_name,
        CASE 
            -- Diabetes
            WHEN condition_concept_name = 'Prediabetes' THEN 'prediabetes'
            WHEN condition_concept_name = 'Type 2 diabetes mellitus' THEN 'diabetes'
            WHEN condition_concept_name = 'Type 2 diabetes mellitus without complication' THEN 'diabetes'
            WHEN condition_concept_name = 'Type 1 diabetes mellitus without complication' THEN 'diabetes'
            WHEN condition_concept_name = 'Type 1 diabetes mellitus uncontrolled' THEN 'diabetes'
            WHEN condition_concept_name = 'Type 1 diabetes mellitus' THEN 'diabetes'
            WHEN condition_concept_name = 'Diabetes mellitus without complication' THEN 'diabetes'
            WHEN condition_concept_name = 'Renal disorder due to type 2 diabetes mellitus' THEN 'diabetes_complications'
            WHEN condition_concept_name = 'Disorder of kidney due to diabetes mellitus' THEN 'diabetes_complications'
            WHEN condition_concept_name = 'Disorder due to type 2 diabetes mellitus' THEN 'diabetes_complications'
            WHEN condition_concept_name = 'Disorder of nervous system due to type 2 diabetes mellitus' THEN 'diabetes_complications'
            WHEN condition_concept_name = 'Polyneuropathy due to type 2 diabetes mellitus' THEN 'diabetes_complications'
            WHEN condition_concept_name = 'Hyperglycemia due to type 2 diabetes mellitus' THEN 'diabetes_complications'
            WHEN condition_concept_name = 'Chronic kidney disease due to type 2 diabetes mellitus' THEN 'diabetes_complications'
            WHEN condition_concept_name = 'Complication due to diabetes mellitus' THEN 'diabetes_complications'
            WHEN condition_concept_name = 'Peripheral circulatory disorder due to type 2 diabetes mellitus' THEN 'diabetes_complications'
            WHEN condition_concept_name = 'Autonomic neuropathy due to type 2 diabetes mellitus' THEN 'diabetes_complications'
            WHEN condition_concept_name = 'Hyperglycemia due to type 1 diabetes mellitus' THEN 'diabetes_complications'
            WHEN condition_concept_name = 'Foot ulcer due to type 2 diabetes mellitus' THEN 'diabetes_complications'
            WHEN condition_concept_name = 'Disorder of eye due to type 2 diabetes mellitus' THEN 'diabetes_complications'
            WHEN condition_concept_name = 'Hypoglycemia due to type 2 diabetes mellitus' THEN 'diabetes_complications'
            -- Chronic kidney disease 
            WHEN condition_concept_name = 'Chronic kidney disease stage 3' THEN 'chronic_kidney_disease'
            WHEN condition_concept_name = 'Chronic kidney disease' THEN 'chronic_kidney_disease'
            WHEN condition_concept_name = 'Chronic kidney disease due to type 2 diabetes mellitus' THEN 'chronic_kidney_disease'
            WHEN condition_concept_name = 'Chronic kidney disease due to hypertension' THEN 'chronic_kidney_disease'
            WHEN condition_concept_name = 'Chronic kidney disease stage 3B' THEN 'chronic_kidney_disease'
            WHEN condition_concept_name = 'Chronic kidney disease stage 4' THEN 'chronic_kidney_disease'
            WHEN condition_concept_name = 'Chronic kidney disease stage 2' THEN 'chronic_kidney_disease'
            WHEN condition_concept_name = 'Hypertensive heart and chronic kidney disease' THEN 'chronic_kidney_disease'
            WHEN condition_concept_name = 'Anemia in chronic kidney disease' THEN 'chronic_kidney_disease'
            WHEN condition_concept_name = 'Chronic kidney disease stage 5' THEN 'chronic_kidney_disease'
            WHEN condition_concept_name = 'Chronic kidney disease stage 3A' THEN 'chronic_kidney_disease'
            WHEN condition_concept_name = 'Chronic kidney disease stage 1' THEN 'chronic_kidney_disease'
            -- Congestive heart failure
            WHEN condition_concept_name = 'Congestive heart failure' THEN 'congestive_heart_failure'
            WHEN condition_concept_name = 'Heart failure' THEN 'congestive_heart_failure'
            WHEN condition_concept_name = 'Systolic heart failure' THEN 'congestive_heart_failure'
            WHEN condition_concept_name = 'Hypertensive heart failure' THEN 'congestive_heart_failure'
            WHEN condition_concept_name = 'Chronic diastolic heart failure' THEN 'congestive_heart_failure'
            WHEN condition_concept_name = 'Chronic combined systolic and diastolic heart failure' THEN 'congestive_heart_failure'
            WHEN condition_concept_name = 'Chronic systolic heart failure' THEN 'congestive_heart_failure'
            WHEN condition_concept_name = 'Chronic congestive heart failure' THEN 'congestive_heart_failure'
            WHEN condition_concept_name = 'Acute on chronic systolic heart failure' THEN 'congestive_heart_failure'
            WHEN condition_concept_name = 'Diastolic heart failure' THEN 'congestive_heart_failure'
            WHEN condition_concept_name = 'Acute on chronic diastolic heart failure' THEN 'congestive_heart_failure'
            WHEN condition_concept_name = 'Hypertensive heart disease without congestive heart failure' THEN 'congestive_heart_failure'
            WHEN condition_concept_name = 'Biventricular congestive heart failure' THEN 'congestive_heart_failure'
            WHEN condition_concept_name = 'Acute systolic heart failure' THEN 'congestive_heart_failure'
            WHEN condition_concept_name = 'Fetal heart finding' THEN 'congestive_heart_failure'
            WHEN condition_concept_name = 'Acute on chronic combined systolic and diastolic heart failure' THEN 'congestive_heart_failure'
            WHEN condition_concept_name = 'Acute right-sided heart failure' THEN 'congestive_heart_failure'
            WHEN condition_concept_name = 'Left heart failure' THEN 'congestive_heart_failure'
            WHEN condition_concept_name = 'Hypertensive heart and renal disease with (congestive) heart failure' THEN 'congestive_heart_failure'
            WHEN condition_concept_name = 'Acute combined systolic and diastolic heart failure' THEN 'congestive_heart_failure'
            -- Chronic pulmonary disease
            WHEN condition_concept_name = 'Chronic pulmonary edema' THEN 'chronic_pulmonary_disease'
            WHEN condition_concept_name = 'Chronic pulmonary embolism' THEN 'chronic_pulmonary_disease'
            WHEN condition_concept_name = 'Chronic pulmonary coccidioidomycosis' THEN 'chronic_pulmonary_disease'
            WHEN condition_concept_name = 'Chronic obstructive pulmonary disease with acute lower respiratory infection' THEN 'chronic_pulmonary_disease'
            WHEN condition_concept_name = 'Chronic cor pulmonale' THEN 'chronic_pulmonary_disease'
        END AS high_level_condition
    FROM condition_occurrence) glc 
ON feat.person_id = glc.person_id and glc.condition_start_date <= feat.pre_window_end_dt 
WHERE 
    glc.high_level_condition is not null
    and (glc.condition_end_date >= feat.pre_window_start_dt or glc.condition_end_date is null)

@transform_pandas(
    Output(rid="ri.vector.main.execute.d357092e-6841-48fb-8d5f-809b4a1ae4fb"),
    Collect_the_Cohort=Input(rid="ri.vector.main.execute.2fef4d8c-9e31-463a-b919-d9ea3e2f78b9"),
    Hospitalized_Cases=Input(rid="ri.vector.main.execute.02850990-abf4-4b63-82b7-dbad6fda7ed7")
)
-- add flag to show whether patients were in the hospitalized group or the non-hospitalized group; this becomes a model feature
SELECT ctc.*,
case when hc.person_id is not null 
    then 'CASE_HOSP' else 'CASE_NONHOSP' end as patient_group
FROM Collect_the_Cohort ctc
LEFT JOIN 
Hospitalized_Cases hc ON ctc.person_id = hc.person_id

@transform_pandas(
    Output(rid="ri.vector.main.execute.8ef58a55-93a1-4cf9-bebd-fac579b61dfb"),
    microvisits_to_macrovisits=Input(rid="ri.foundry.main.dataset.f5008fa4-e736-4244-88e1-1da7a68efcdb")
)
SELECT * 
        FROM microvisits_to_macrovisits
        WHERE visit_concept_name IN ('Inpatient Visit', 'Inpatient Hospital', 'Inpatient Critical Care Facility', 
        'Emergency Room and Inpatient Visit', 
        'Emergency Room Visit', 'Intensive Care', 'Emergency Room - Hospital') 
        and macrovisit_id is not null

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.26ad74be-fcb8-4a41-b373-501854feb6c3"),
    four_windows_measure=Input(rid="ri.vector.main.execute.2b963a2e-59b2-480a-9d86-553513b25a89")
)
SELECT 
	nvl(nvl(nvl(pre_pre_person_id, pre_person_id), covid_person_id), post_person_id) as person_id, 
	nvl(nvl(nvl(pre_pre_measurement_concept_name, pre_measurement_concept_name), covid_measurement_concept_name), post_measurement_concept_name) as measurement_concept_name, 
	nvl(nvl(nvl(pre_pre_measurement_concept_id, pre_measurement_concept_id), covid_measurement_concept_id), post_measurement_concept_id) as measurement_concept_id, 
    pre_pre_max as pre_pre_max, 
    pre_pre_min as pre_pre_min, 
    pre_pre_avg as pre_pre_avg, 

    pre_max as pre_max,
    pre_min as pre_min,
    pre_avg as pre_avg,

    covid_max as covid_max,
    covid_min as covid_min,
    covid_avg as covid_avg,
    post_max as post_max,
    post_min as post_min,
    post_avg as post_avg
FROM four_windows_measure

@transform_pandas(
    Output(rid="ri.vector.main.execute.56bdc9d2-12a2-4e87-be07-65b3c2b7783d"),
    Feature_table_builder=Input(rid="ri.foundry.main.dataset.e0264d38-57aa-4509-9245-3d008b0526c4"),
    observation=Input(rid="ri.foundry.main.dataset.fc1ce22e-9cf6-4335-8ca7-aa8c733d506d")
)
SELECT o.person_id, CASE WHEN o.observation_concept_name == 'Marital status [NHANES]' THEN first(o.value_as_concept_name)
                    ELSE o.observation_concept_name END AS observation_concept_name
FROM observation o
JOIN Feature_table_builder f
ON o.person_id = f.person_id
WHERE o.observation_concept_name IN ('Never smoker', 
'Former smoker', 'Current every day smoker', 'Long-term current use of insulin', 
'Long-term current use of anticoagulant', 'Denies alcohol use', 'Admits alcohol use', 
'Body mass index 30+ - obesity', 'Body mass index 40+ - severely obese',
'Marital status [NHANES]', 'Dependence on renal dialysis')
AND (o.value_as_concept_name IS NOT NULL OR o.observation_concept_name NOT IN ('Marital status [NHANES]'))
GROUP BY o.person_id, observation_concept_name

@transform_pandas(
    Output(rid="ri.vector.main.execute.9baea4ea-ef0d-4496-bacf-c34d0cba82f7"),
    obs_person=Input(rid="ri.vector.main.execute.56bdc9d2-12a2-4e87-be07-65b3c2b7783d")
)
SELECT * 
FROM obs_person op
WHERE op.observation_concept_name IN
(SELECT op.observation_concept_name
FROM obs_person op
GROUP BY observation_concept_name
HAVING COUNT(*) > 10) -- exclude sparse concepts

@transform_pandas(
    Output(rid="ri.vector.main.execute.2cf5493c-fc3e-40da-a018-52f37934e2fe"),
    covid_window=Input(rid="ri.vector.main.execute.518c5020-2537-4606-a367-d0ec24710869"),
    post_covid=Input(rid="ri.vector.main.execute.af40d95d-42dc-4fe2-a8f6-4fd87d1f703c")
)
-- first (min) postive date, last (max) postive date, first negative date after the first positive date, number of tests
-- covid
SELECT t.*, min(c.measure_neg_date) as first_neg_dt, 'covid' as window_type
FROM covid_window c
LEFT JOIN 
(SELECT c.person_id, c.measure_type, 
    min(measure_pos_date) as first_pos_dt,
    max(measure_pos_date) as last_pos_dt
FROM covid_window c
GROUP BY c.person_id, c.measure_type) t
ON c.person_id = t.person_id AND c.measure_type = t.measure_type
WHERE c.measure_neg_date > t.first_pos_dt -- first negative date after the positive date
    OR c.measure_neg_date IS NULL 
GROUP BY t.person_id, t.measure_type, t.first_pos_dt, t.last_pos_dt

UNION
-- post-covid
SELECT t.*, min(p.measure_neg_date) as first_neg_dt, 'pos_covid' as window_type
FROM post_covid p
LEFT JOIN 
(SELECT p.person_id, p.measure_type, 
    min(measure_pos_date) as first_pos_dt,
    max(measure_pos_date) as last_pos_dt
FROM post_covid p
GROUP BY p.person_id, p.measure_type) t
ON p.person_id = t.person_id AND p.measure_type = t.measure_type
WHERE p.measure_neg_date > t.first_pos_dt -- first negative date after the positive date
    OR p.measure_neg_date IS NULL 
GROUP BY t.person_id, t.measure_type, t.first_pos_dt, t.last_pos_dt

@transform_pandas(
    Output(rid="ri.vector.main.execute.0fe62ec5-fdaf-482e-aed0-df5e7ea4130d"),
    Feature_table_builder=Input(rid="ri.foundry.main.dataset.e0264d38-57aa-4509-9245-3d008b0526c4"),
    condition_occurrence=Input(rid="ri.foundry.main.dataset.3e01546f-f110-4c67-a6db-9063d2939a74")
)
-- find all conditions associated with patients in their post window
SELECT feat.*, co.condition_concept_name, co.condition_concept_id, co.condition_start_date, co.condition_source_value, co.visit_occurrence_id
FROM Feature_table_builder feat 
JOIN 
condition_occurrence co ON feat.person_id = co.person_id and co.condition_start_date between feat.post_window_start_dt and feat.post_window_end_dt

@transform_pandas(
    Output(rid="ri.vector.main.execute.af40d95d-42dc-4fe2-a8f6-4fd87d1f703c"),
    covid_person=Input(rid="ri.foundry.main.dataset.e5aa8575-e263-4cd1-97cd-a768cb7f975c")
)
SELECT cp.person_id, cp.measurement_concept_name as measure_type,
    CASE WHEN cp.pos_or_neg = 1
    THEN cp.measurement_date
    ELSE NULL
    END as measure_pos_date,
    CASE WHEN cp.pos_or_neg = 0
    THEN cp.measurement_date
    ELSE NULL
    END as measure_neg_date
FROM covid_person cp
WHERE cp.measurement_date > cp.post_window_start_dt and cp.measurement_date <= cp.post_window_end_dt

@transform_pandas(
    Output(rid="ri.vector.main.execute.c3dc502d-6732-4c94-a137-5ba0259fefac"),
    Feature_table_builder=Input(rid="ri.foundry.main.dataset.e0264d38-57aa-4509-9245-3d008b0526c4"),
    drugRollUp=Input(rid="ri.vector.main.execute.e79d57fa-0fc2-4e7a-9940-603568a40ae9")
)
-- pull all the drugs associated with the patient in their post window
SELECT feat.*, co.ancestor_drug_concept_name, co.ancestor_drug_concept_id, co.drug_exposure_start_date, co.visit_occurrence_id
FROM Feature_table_builder feat JOIN drugRollUp co ON feat.person_id = co.person_id and co.drug_exposure_start_date between feat.post_window_start_dt and feat.post_window_end_dt

@transform_pandas(
    Output(rid="ri.vector.main.execute.c247cc21-4fbc-419c-affe-bc7d560dd149"),
    Feature_table_builder=Input(rid="ri.foundry.main.dataset.e0264d38-57aa-4509-9245-3d008b0526c4"),
    measurement_person=Input(rid="ri.vector.main.execute.b58177f1-cd68-4be9-be4b-715f899c2a0b")
)
-- find all conditions associated with patients in their pre-pre window
SELECT feat.person_id, m.measurement_concept_name, m.measurement_concept_id, 
max(m.harmonized_value_as_number) as max_measure, min(m.harmonized_value_as_number) as min_measure, 
avg(m.harmonized_value_as_number) as avg_measure
FROM Feature_table_builder feat 
JOIN 
measurement_person m ON feat.person_id = m.person_id and m.visit_date between feat.post_window_start_dt and feat.post_window_end_dt
GROUP BY feat.person_id, m.measurement_concept_name, m.measurement_concept_id

@transform_pandas(
    Output(rid="ri.vector.main.execute.c69da2fd-2170-410e-bba6-be51c2b3f852"),
    Feature_table_builder=Input(rid="ri.foundry.main.dataset.e0264d38-57aa-4509-9245-3d008b0526c4"),
    microvisits_to_macrovisits=Input(rid="ri.foundry.main.dataset.f5008fa4-e736-4244-88e1-1da7a68efcdb"),
    post_drugs=Input(rid="ri.vector.main.execute.c3dc502d-6732-4c94-a137-5ba0259fefac")
)
SELECT feat.person_id, feat.patient_group, prc.ancestor_drug_concept_name, prc.ancestor_drug_concept_id, 'post count' as count_type, count(distinct nvl(mml.macrovisit_id, mml.visit_occurrence_id)) as med_count
	FROM Feature_table_builder feat 
    		JOIN microvisits_to_macrovisits mml ON feat.person_id = mml.person_id
    		JOIN post_drugs prc ON mml.visit_occurrence_id = prc.visit_occurrence_id
	GROUP BY 
	feat.person_id, feat.patient_group, prc.ancestor_drug_concept_name, prc.ancestor_drug_concept_id

@transform_pandas(
    Output(rid="ri.vector.main.execute.7941d9bb-4846-4863-8b22-6013668847b4"),
    Feature_table_builder=Input(rid="ri.foundry.main.dataset.e0264d38-57aa-4509-9245-3d008b0526c4"),
    condition_occurrence=Input(rid="ri.foundry.main.dataset.3e01546f-f110-4c67-a6db-9063d2939a74")
)
-- find all conditions associated with patients in their pre window
SELECT feat.*, co.condition_concept_name, co.condition_concept_id, co.condition_start_date, co.condition_source_value, co.visit_occurrence_id
FROM Feature_table_builder feat 
JOIN 
condition_occurrence co ON feat.person_id = co.person_id and co.condition_start_date between feat.pre_window_start_dt and feat.pre_window_end_dt

@transform_pandas(
    Output(rid="ri.vector.main.execute.cda9dabb-7a23-4cc8-8eb9-6e5abe4b3fe6"),
    Feature_table_builder=Input(rid="ri.foundry.main.dataset.e0264d38-57aa-4509-9245-3d008b0526c4"),
    drugRollUp=Input(rid="ri.vector.main.execute.e79d57fa-0fc2-4e7a-9940-603568a40ae9")
)
-- pull all the drugs associated with the patient in their pre window
SELECT feat.*, co.ancestor_drug_concept_name, co.ancestor_drug_concept_id, co.drug_exposure_start_date, co.visit_occurrence_id
FROM Feature_table_builder feat JOIN drugRollUp co ON feat.person_id = co.person_id and co.drug_exposure_start_date between feat.pre_window_start_dt and feat.pre_window_end_dt

@transform_pandas(
    Output(rid="ri.vector.main.execute.09fc9cc5-5778-4c21-90a7-8b8550a63f10"),
    Feature_table_builder=Input(rid="ri.foundry.main.dataset.e0264d38-57aa-4509-9245-3d008b0526c4"),
    measurement_person=Input(rid="ri.vector.main.execute.b58177f1-cd68-4be9-be4b-715f899c2a0b")
)
-- find all conditions associated with patients in their pre-pre window
SELECT feat.person_id, m.measurement_concept_name, m.measurement_concept_id, 
max(m.harmonized_value_as_number) as max_measure, min(m.harmonized_value_as_number) as min_measure, 
avg(m.harmonized_value_as_number) as avg_measure
FROM Feature_table_builder feat 
JOIN 
measurement_person m ON feat.person_id = m.person_id and m.visit_date between feat.pre_window_start_dt and feat.pre_window_end_dt
GROUP BY feat.person_id, m.measurement_concept_name, m.measurement_concept_id

@transform_pandas(
    Output(rid="ri.vector.main.execute.73433ca0-fec4-4210-a0b9-2946683617bc"),
    Feature_table_builder=Input(rid="ri.foundry.main.dataset.e0264d38-57aa-4509-9245-3d008b0526c4"),
    four_windows_dx_counts=Input(rid="ri.vector.main.execute.c65dde9f-1306-48e5-8bde-748eaec89f88")
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
    Output(rid="ri.vector.main.execute.b7524cd5-89b3-43b9-9eef-5db82836ffef"),
    covidtbl=Input(rid="ri.vector.main.execute.dea2bc63-0ae2-4699-81d4-41fdf7f188b2"),
    posttbl=Input(rid="ri.vector.main.execute.c69da2fd-2170-410e-bba6-be51c2b3f852"),
    prepretbl=Input(rid="ri.vector.main.execute.09ac4344-c222-4e01-844c-c175483401fd"),
    pretbl=Input(rid="ri.vector.main.execute.908b30b4-244d-4c6f-82fb-c9847e3ba74f")
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
    Output(rid="ri.foundry.main.dataset.c729b93f-caf1-4a7d-ac0d-569188c4526e"),
    Feature_table_builder=Input(rid="ri.foundry.main.dataset.e0264d38-57aa-4509-9245-3d008b0526c4"),
    pre_post_med_count=Input(rid="ri.vector.main.execute.b7524cd5-89b3-43b9-9eef-5db82836ffef")
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
    Output(rid="ri.vector.main.execute.3e73242f-f520-4115-9631-bdbd0d73e6e0"),
    Feature_table_builder=Input(rid="ri.foundry.main.dataset.e0264d38-57aa-4509-9245-3d008b0526c4"),
    condition_occurrence=Input(rid="ri.foundry.main.dataset.3e01546f-f110-4c67-a6db-9063d2939a74")
)
-- find all conditions associated with patients in their pre-pre window
SELECT feat.*, co.condition_concept_name, co.condition_concept_id, co.condition_start_date, co.condition_source_value, co.visit_occurrence_id
FROM Feature_table_builder feat 
JOIN 
condition_occurrence co ON feat.person_id = co.person_id and co.condition_start_date between feat.pre_pre_window_start_dt and feat.pre_window_start_dt

@transform_pandas(
    Output(rid="ri.vector.main.execute.ebe96072-83d0-4281-bb16-d0422a63f93c"),
    Feature_table_builder=Input(rid="ri.foundry.main.dataset.e0264d38-57aa-4509-9245-3d008b0526c4"),
    drugRollUp=Input(rid="ri.vector.main.execute.e79d57fa-0fc2-4e7a-9940-603568a40ae9")
)
-- pull all the drugs associated with the patient in their pre window
SELECT feat.*, co.ancestor_drug_concept_name, co.ancestor_drug_concept_id, co.drug_exposure_start_date, co.visit_occurrence_id
FROM Feature_table_builder feat JOIN drugRollUp co ON feat.person_id = co.person_id and co.drug_exposure_start_date between feat.pre_pre_window_start_dt and feat.pre_window_start_dt

@transform_pandas(
    Output(rid="ri.vector.main.execute.c49ca582-65b0-49de-822d-55287579ddf8"),
    Feature_table_builder=Input(rid="ri.foundry.main.dataset.e0264d38-57aa-4509-9245-3d008b0526c4"),
    measurement_person=Input(rid="ri.vector.main.execute.b58177f1-cd68-4be9-be4b-715f899c2a0b")
)
-- find all conditions associated with patients in their pre-pre window
SELECT feat.person_id, m.measurement_concept_name, m.measurement_concept_id, 
max(m.harmonized_value_as_number) as max_measure, min(m.harmonized_value_as_number) as min_measure, 
avg(m.harmonized_value_as_number) as avg_measure
FROM Feature_table_builder feat 
JOIN 
measurement_person m ON feat.person_id = m.person_id and m.visit_date between feat.pre_pre_window_start_dt and feat.pre_window_start_dt
GROUP BY feat.person_id, m.measurement_concept_name, m.measurement_concept_id

@transform_pandas(
    Output(rid="ri.vector.main.execute.09ac4344-c222-4e01-844c-c175483401fd"),
    Feature_table_builder=Input(rid="ri.foundry.main.dataset.e0264d38-57aa-4509-9245-3d008b0526c4"),
    microvisits_to_macrovisits=Input(rid="ri.foundry.main.dataset.f5008fa4-e736-4244-88e1-1da7a68efcdb"),
    pre_pre_drugs=Input(rid="ri.vector.main.execute.ebe96072-83d0-4281-bb16-d0422a63f93c")
)
SELECT feat.person_id, feat.patient_group, prc.ancestor_drug_concept_name, prc.ancestor_drug_concept_id, 'pre pre count' as count_type, count(distinct nvl(mml.macrovisit_id, mml.visit_occurrence_id)) as med_count
	FROM Feature_table_builder feat 
    		JOIN microvisits_to_macrovisits mml ON feat.person_id = mml.person_id
    		JOIN pre_pre_drugs prc ON mml.visit_occurrence_id = prc.visit_occurrence_id
	GROUP BY 
	feat.person_id, feat.patient_group, prc.ancestor_drug_concept_name, prc.ancestor_drug_concept_id

@transform_pandas(
    Output(rid="ri.vector.main.execute.908b30b4-244d-4c6f-82fb-c9847e3ba74f"),
    Feature_table_builder=Input(rid="ri.foundry.main.dataset.e0264d38-57aa-4509-9245-3d008b0526c4"),
    microvisits_to_macrovisits=Input(rid="ri.foundry.main.dataset.f5008fa4-e736-4244-88e1-1da7a68efcdb"),
    pre_drugs=Input(rid="ri.vector.main.execute.cda9dabb-7a23-4cc8-8eb9-6e5abe4b3fe6")
)
SELECT feat.person_id, feat.patient_group, prc.ancestor_drug_concept_name, prc.ancestor_drug_concept_id, 'pre count' as count_type, count(distinct nvl(mml.macrovisit_id, mml.visit_occurrence_id)) as med_count
	FROM Feature_table_builder feat 
    		JOIN microvisits_to_macrovisits mml ON feat.person_id = mml.person_id
    		JOIN pre_drugs prc ON mml.visit_occurrence_id = prc.visit_occurrence_id
	GROUP BY 
	feat.person_id, feat.patient_group, prc.ancestor_drug_concept_name, prc.ancestor_drug_concept_id

@transform_pandas(
    Output(rid="ri.vector.main.execute.fde6567c-4e53-42e9-b07a-d733fb443471"),
    Feature_Table_Builder_v0=Input(rid="ri.vector.main.execute.e167e968-4f04-4205-8339-0336930dd83f"),
    ICU_visits=Input(rid="ri.vector.main.execute.192a0bb9-7930-4f8a-8227-405ff5d0f6a0")
)
-- find the total number of inpatient days in the covid window and the post covid window for use later
SELECT nvl(post_tbl.person_id, covid_tbl.person_id) as person_id, nvl(post_tbl.post_window_start_dt, covid_tbl.post_window_start_dt) as post_window_start_dt, post_tbl.post_window_end_dt, post_tot_icu_days, covid_tbl.pre_window_end_dt, covid_tot_icu_days
FROM

(SELECT *,      
        CASE WHEN datediff(macrovisit_end_date,macrovisit_start_date) >  datediff(post_window_end_dt, macrovisit_start_date)
         THEN datediff(post_window_end_dt, macrovisit_start_date)  ELSE datediff(macrovisit_end_date,macrovisit_start_date) 
         END post_tot_icu_days
FROM(SELECT 
    person_id, post_window_start_dt, post_window_end_dt, max(macrovisit_end_date) as macrovisit_end_date, min(macrovisit_start_date) as macrovisit_start_date
FROM 	(
	SELECT distinct feat.person_id, 
	feat.post_window_start_dt,
	feat.post_window_end_dt,
	mm.macrovisit_start_date, 
	mm.macrovisit_end_date
	FROM Feature_Table_Builder_v0 feat JOIN ICU_visits mm 
    ON feat.person_id = mm.person_id and mm.macrovisit_start_date between feat.post_window_start_dt and feat.post_window_end_dt
	) tbl
GROUP BY person_id, post_window_start_dt, post_window_end_dt)
) post_tbl

FULL JOIN

(SELECT *,      
        CASE WHEN datediff(macrovisit_end_date,macrovisit_start_date) >  datediff(post_window_start_dt, macrovisit_start_date)
         THEN datediff(post_window_start_dt, macrovisit_start_date)  ELSE datediff(macrovisit_end_date,macrovisit_start_date) 
         END covid_tot_icu_days
FROM(SELECT 
    person_id, pre_window_end_dt, post_window_start_dt, max(macrovisit_end_date) as macrovisit_end_date, min(macrovisit_start_date) as macrovisit_start_date
FROM 	(
	SELECT distinct feat.person_id, 
	feat.pre_window_end_dt,
	feat.post_window_start_dt,
	mm.macrovisit_start_date, 
	mm.macrovisit_end_date
	FROM Feature_Table_Builder_v0 feat JOIN ICU_visits mm 
    ON feat.person_id = mm.person_id and mm.macrovisit_start_date between feat.pre_window_end_dt and feat.post_window_start_dt
	) tbl
GROUP BY person_id, pre_window_end_dt, post_window_start_dt)
) covid_tbl

ON post_tbl.person_id = covid_tbl.person_id and post_tbl.post_window_start_dt = covid_tbl.post_window_start_dt

@transform_pandas(
    Output(rid="ri.vector.main.execute.692f0334-6976-4043-9038-27e2d8836f79"),
    Feature_Table_Builder_v0=Input(rid="ri.vector.main.execute.e167e968-4f04-4205-8339-0336930dd83f"),
    inpatient_visits=Input(rid="ri.vector.main.execute.8ef58a55-93a1-4cf9-bebd-fac579b61dfb")
)
-- find the total number of inpatient days in the covid window and the post covid window for use later
SELECT nvl(post_tbl.person_id, covid_tbl.person_id) as person_id, nvl(post_tbl.post_window_start_dt, covid_tbl.post_window_start_dt) as post_window_start_dt, post_tbl.post_window_end_dt, post_tot_ip_days, covid_tbl.pre_window_end_dt, covid_tot_ip_days
FROM

(SELECT *,      
        CASE WHEN datediff(macrovisit_end_date,macrovisit_start_date) >  datediff(post_window_end_dt, macrovisit_start_date)
         THEN datediff(post_window_end_dt, macrovisit_start_date)  ELSE datediff(macrovisit_end_date,macrovisit_start_date) 
         END post_tot_ip_days
FROM(SELECT 
    person_id, post_window_start_dt, post_window_end_dt, max(macrovisit_end_date) as macrovisit_end_date, min(macrovisit_start_date) as macrovisit_start_date
FROM 	(
	SELECT distinct feat.person_id, 
	feat.post_window_start_dt,
	feat.post_window_end_dt,
	mm.macrovisit_start_date, 
	mm.macrovisit_end_date
	FROM Feature_Table_Builder_v0 feat JOIN inpatient_visits mm 
    ON feat.person_id = mm.person_id and mm.macrovisit_start_date between feat.post_window_start_dt and feat.post_window_end_dt
	) tbl
GROUP BY person_id, post_window_start_dt, post_window_end_dt)
) post_tbl

FULL JOIN

(SELECT *,      
        CASE WHEN datediff(macrovisit_end_date,macrovisit_start_date) >  datediff(post_window_start_dt, macrovisit_start_date)
         THEN datediff(post_window_start_dt, macrovisit_start_date)  ELSE datediff(macrovisit_end_date,macrovisit_start_date) 
         END covid_tot_ip_days
FROM(SELECT 
    person_id, pre_window_end_dt, post_window_start_dt, max(macrovisit_end_date) as macrovisit_end_date, min(macrovisit_start_date) as macrovisit_start_date
FROM 	(
	SELECT distinct feat.person_id, 
	feat.pre_window_end_dt,
	feat.post_window_start_dt,
	mm.macrovisit_start_date, 
	mm.macrovisit_end_date
	FROM Feature_Table_Builder_v0 feat JOIN inpatient_visits mm 
    ON feat.person_id = mm.person_id and mm.macrovisit_start_date between feat.pre_window_end_dt and feat.post_window_start_dt
	) tbl
GROUP BY person_id, pre_window_end_dt, post_window_start_dt)
) covid_tbl

ON post_tbl.person_id = covid_tbl.person_id and post_tbl.post_window_start_dt = covid_tbl.post_window_start_dt

