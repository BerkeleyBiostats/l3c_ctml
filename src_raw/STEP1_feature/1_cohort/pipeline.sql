

@transform_pandas(
    Output(rid="ri.vector.main.execute.b394ad88-ebb0-4f13-bf86-cfa6a7f5e612"),
    Covid_Pasc_Index_Dates=Input(rid="ri.vector.main.execute.354cc0eb-336b-4864-b750-9d75bf0a8ba4"),
    manifest_safe_harbor=Input(rid="ri.foundry.main.dataset.b4407989-1851-4e07-a13f-0539fae10f26"),
    person=Input(rid="ri.foundry.main.dataset.f71ffe18-6969-4a24-b81c-0e06a1ae9316")
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
FROM Covid_Pasc_Index_Dates vc 
    JOIN person pr ON vc.person_id = pr.person_id
    LEFT JOIN manifest_safe_harbor msh ON pr.data_partner_id = msh.data_partner_id 	-- SITE_INFO: Joining to the manifest table here
-- WHERE
--     datediff(msh.run_date, array_min(array(covid_index, d.death_date))) >= 100

@transform_pandas(
    Output(rid="ri.vector.main.execute.354cc0eb-336b-4864-b750-9d75bf0a8ba4"),
    Long_COVID_Silver_Standard=Input(rid="ri.foundry.main.dataset.3ea1038c-e278-4b0e-8300-db37d3505671")
)
SELECT l.*,
    date_add(covid_index, time_to_pasc) as pasc_index
FROM Long_COVID_Silver_Standard l
WHERE pasc_code_prior_four_weeks != 1 -- exclude patients positive prior code

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.ce7a93a0-4140-4fdb-b97d-fb78c0caf345"),
    Feature_Table_Builder_v0=Input(rid="ri.vector.main.execute.e26f3947-ea85-4de9-b662-4048a52ec048"),
    tot_icu_days_calc=Input(rid="ri.vector.main.execute.e8f9f7e0-1c42-44d6-8fcd-20cc54971623"),
    tot_ip_days_calc=Input(rid="ri.vector.main.execute.fe1ce00c-f84c-4fc6-b1bb-d3a268301ade")
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
    Output(rid="ri.vector.main.execute.e26f3947-ea85-4de9-b662-4048a52ec048"),
    Covid_Pasc_Index_Dates=Input(rid="ri.vector.main.execute.354cc0eb-336b-4864-b750-9d75bf0a8ba4"),
    hosp_and_non=Input(rid="ri.vector.main.execute.a20c0955-295e-48b1-9286-81621279712f"),
    manifest_safe_harbor=Input(rid="ri.foundry.main.dataset.b4407989-1851-4e07-a13f-0539fae10f26"),
    microvisits_to_macrovisits=Input(rid="ri.foundry.main.dataset.d77a701f-34df-48a1-a71c-b28112a07ffa")
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
    			JOIN Covid_Pasc_Index_Dates lc ON hc.person_id = lc.person_id
		GROUP BY
		hc.person_id, hc.patient_group, hc.apprx_age, hc.sex, hc.race, hc.ethn, hc.site_id, hc.min_covid_dt, msh.run_date, lc.pasc_index
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
    Output(rid="ri.vector.main.execute.fafe2849-680c-4e7c-bd60-bc474da15887"),
    Collect_the_Cohort=Input(rid="ri.vector.main.execute.b394ad88-ebb0-4f13-bf86-cfa6a7f5e612"),
    condition_occurrence=Input(rid="ri.foundry.main.dataset.2f496793-6a4e-4bf4-b0fc-596b277fb7e2"),
    microvisits_to_macrovisits=Input(rid="ri.foundry.main.dataset.d77a701f-34df-48a1-a71c-b28112a07ffa")
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
    Output(rid="ri.vector.main.execute.e86d4e39-4ce0-4b57-b3ec-921a86640b88"),
    microvisits_to_macrovisits=Input(rid="ri.foundry.main.dataset.d77a701f-34df-48a1-a71c-b28112a07ffa")
)
SELECT distinct 
    person_id, visit_concept_name, macrovisit_id, macrovisit_start_date, macrovisit_end_date
        FROM microvisits_to_macrovisits
        WHERE visit_concept_name IN ( 'Inpatient Critical Care Facility', 
        'Emergency Room and Inpatient Visit', 
        'Emergency Room Visit', 'Intensive Care', 'Emergency Room - Hospital')
        and macrovisit_id is not null

@transform_pandas(
    Output(rid="ri.vector.main.execute.a20c0955-295e-48b1-9286-81621279712f"),
    Collect_the_Cohort=Input(rid="ri.vector.main.execute.b394ad88-ebb0-4f13-bf86-cfa6a7f5e612"),
    Hospitalized_Cases=Input(rid="ri.vector.main.execute.fafe2849-680c-4e7c-bd60-bc474da15887")
)
-- add flag to show whether patients were in the hospitalized group or the non-hospitalized group; this becomes a model feature
SELECT ctc.*,
case when hc.person_id is not null 
    then 'CASE_HOSP' else 'CASE_NONHOSP' end as patient_group
FROM Collect_the_Cohort ctc
LEFT JOIN 
Hospitalized_Cases hc ON ctc.person_id = hc.person_id

@transform_pandas(
    Output(rid="ri.vector.main.execute.3853f0d6-ac95-4675-bbd2-5a33395676ef"),
    microvisits_to_macrovisits=Input(rid="ri.foundry.main.dataset.d77a701f-34df-48a1-a71c-b28112a07ffa")
)
SELECT * 
        FROM microvisits_to_macrovisits
        WHERE visit_concept_name IN ('Inpatient Visit', 'Inpatient Hospital', 'Inpatient Critical Care Facility', 
        'Emergency Room and Inpatient Visit', 
        'Emergency Room Visit', 'Intensive Care', 'Emergency Room - Hospital') 
        and macrovisit_id is not null

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.34a5ed27-4c8c-49ae-b084-73bd73c79a49"),
    Covid_Pasc_Index_Dates=Input(rid="ri.vector.main.execute.354cc0eb-336b-4864-b750-9d75bf0a8ba4")
)
SELECT person_id, pasc_code_after_four_weeks as long_covid, pasc_index
FROM Covid_Pasc_Index_Dates
WHERE pasc_code_prior_four_weeks != 1 -- exclude patients positive prior code

@transform_pandas(
    Output(rid="ri.vector.main.execute.e8f9f7e0-1c42-44d6-8fcd-20cc54971623"),
    Feature_Table_Builder_v0=Input(rid="ri.vector.main.execute.e26f3947-ea85-4de9-b662-4048a52ec048"),
    ICU_visits=Input(rid="ri.vector.main.execute.e86d4e39-4ce0-4b57-b3ec-921a86640b88")
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
    Output(rid="ri.vector.main.execute.fe1ce00c-f84c-4fc6-b1bb-d3a268301ade"),
    Feature_Table_Builder_v0=Input(rid="ri.vector.main.execute.e26f3947-ea85-4de9-b662-4048a52ec048"),
    inpatient_visits=Input(rid="ri.vector.main.execute.3853f0d6-ac95-4675-bbd2-5a33395676ef")
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

