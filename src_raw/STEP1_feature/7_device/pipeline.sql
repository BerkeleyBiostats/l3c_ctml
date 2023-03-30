

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.8599371f-a219-4a0d-be3a-345e200197be"),
    device_covid=Input(rid="ri.foundry.main.dataset.c71ca392-9117-4940-a9ba-0ec468ed35f7"),
    device_filtered=Input(rid="ri.vector.main.execute.c4f0173a-942b-4c1e-b6b4-b1b7183e0d91"),
    device_post=Input(rid="ri.foundry.main.dataset.9e04d829-d626-4640-b157-1633e8dec074"),
    device_pre=Input(rid="ri.foundry.main.dataset.86a8ce34-1c75-4e4c-8512-c0b3dfb2ba4e"),
    device_pre_pre=Input(rid="ri.foundry.main.dataset.6027a85f-b97f-40ea-b125-92f65183484e")
)
-- OLD QUERY WAS TAKING HOURS TO RUN OR WAS COMPLETELY BREAKING. QUERY HAS BEEN REFACTORED
SELECT DISTINCT
    D.person_id,
    D.device_concept_name,
    IFNULL(prepre.prepre_device_count, 0) AS prepre_device_count,
    IFNULL(pre.pre_device_count, 0) AS pre_device_count,
    IFNULL(cov.covid_device_count, 0) AS covid_device_count,
    IFNULL(post.post_device_count, 0) AS post_device_count
FROM
(SELECT DISTINCT
    person_id, device_concept_name
FROM device_filtered) AS D
LEFT JOIN
    device_covid cov
ON D.person_id=cov.person_id AND D.device_concept_name=cov.device_concept_name
LEFT JOIN
    device_pre pre
ON pre.person_id=D.person_id AND pre.device_concept_name=D.device_concept_name
LEFT JOIN
    device_pre_pre prepre
ON prepre.person_id=D.person_id AND prepre.device_concept_name=D.device_concept_name
LEFT JOIN
    device_post post
ON post.person_id=D.person_id AND post.device_concept_name=D.device_concept_name
WHERE NOT (
    prepre.prepre_device_count IS NULL AND
    pre.pre_device_count IS NULL AND
    cov.covid_device_count IS NULL AND
    post.post_device_count IS NULL
)

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.3dedc0a1-f9dc-441c-9f1c-eee2ee5176ba"),
    Device_Count=Input(rid="ri.foundry.main.dataset.8599371f-a219-4a0d-be3a-345e200197be")
)
select distinct tbl.*
FROM Device_Count tbl

@transform_pandas(
    Output(rid="ri.vector.main.execute.35e1f908-2de8-491c-adee-1dc194672d55")
)
-- ORIGINAL CODE FROM TEAM
select distinct tbl.*
FROM Feature_table_builder feat 
JOIN
(SELECT 
-- COALESCE removes the nested properties of these nvl.
nvl(nvl(nvl(prepre_person_id, pre_person_id), covid_person_id), post_person_id) as person_id, 
nvl(nvl(nvl(prepre_device_concept_name, pre_device_concept_name), covid_device_concept_name), post_device_concept_name) as device_concept_name, 

--person_id, 
--device_concept_name, 
nvl(prepre_device_count, 0) as pre_pre_device_count,
nvl(pre_device_count, 0) as pre_device_count,
nvl(covid_device_count, 0) as covid_device_count,
nvl(post_device_count, 0) as post_device_count
FROM device_count
) tbl 
ON feat.person_id = tbl.person_id
WHERE tbl.device_concept_name is not null

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.bac2e802-7da6-4faf-a887-5d0e7e03a67b")
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
    Output(rid="ri.foundry.main.dataset.c71ca392-9117-4940-a9ba-0ec468ed35f7"),
    Feature_table_builder=Input(rid="ri.foundry.main.dataset.ce7a93a0-4140-4fdb-b97d-fb78c0caf345"),
    device_filtered=Input(rid="ri.vector.main.execute.c4f0173a-942b-4c1e-b6b4-b1b7183e0d91")
)
select distinct d.person_id, d.device_concept_name, 1 as covid_device_count
FROM device_filtered d
    JOIN 
    (SELECT person_id, pre_window_end_dt, post_window_start_dt FROM Feature_table_builder) feat
    ON d.person_id = feat.person_id 
       and ((d.device_exposure_start_date between feat.pre_window_end_dt and feat.post_window_start_dt) 
             or (d.device_exposure_end_date between feat.pre_window_end_dt and feat.post_window_start_dt) 
             or (d.device_exposure_start_date < feat.pre_window_end_dt and d.device_exposure_end_date > feat.post_window_start_dt))

@transform_pandas(
    Output(rid="ri.vector.main.execute.c4f0173a-942b-4c1e-b6b4-b1b7183e0d91"),
    Feature_table_builder=Input(rid="ri.foundry.main.dataset.ce7a93a0-4140-4fdb-b97d-fb78c0caf345"),
    device_exposure=Input(rid="ri.foundry.main.dataset.c1fd6d67-fc80-4747-89ca-8eb04efcb874")
)
SELECT d.person_id, d.device_exposure_start_date, d.device_exposure_end_date, d.device_concept_name 
    FROM device_exposure d
WHERE device_concept_name IN('Ventilator', 'Basic nasal oxygen cannula', 'High flow oxygen nasal cannula', 'N3C:Other oxygen device')

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.9e04d829-d626-4640-b157-1633e8dec074"),
    Feature_table_builder=Input(rid="ri.foundry.main.dataset.ce7a93a0-4140-4fdb-b97d-fb78c0caf345"),
    device_filtered=Input(rid="ri.vector.main.execute.c4f0173a-942b-4c1e-b6b4-b1b7183e0d91")
)
select distinct d.person_id, d.device_concept_name, 1 as post_device_count
FROM device_filtered d
    JOIN 
    (SELECT person_id, post_window_start_dt, post_window_end_dt FROM Feature_table_builder) feat
    ON d.person_id = feat.person_id 
       and ((d.device_exposure_start_date between feat.post_window_start_dt and feat.post_window_end_dt) 
             or (d.device_exposure_end_date between feat.post_window_start_dt and feat.post_window_end_dt) 
             or (d.device_exposure_start_date < feat.post_window_start_dt and d.device_exposure_end_date > feat.post_window_end_dt))

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.86a8ce34-1c75-4e4c-8512-c0b3dfb2ba4e"),
    Feature_table_builder=Input(rid="ri.foundry.main.dataset.ce7a93a0-4140-4fdb-b97d-fb78c0caf345"),
    device_filtered=Input(rid="ri.vector.main.execute.c4f0173a-942b-4c1e-b6b4-b1b7183e0d91")
)
select distinct d.person_id, d.device_concept_name, 1 as pre_device_count
FROM device_filtered d
    JOIN 
    (SELECT person_id, pre_window_start_dt, pre_window_end_dt FROM Feature_table_builder) feat
    ON d.person_id = feat.person_id 
       and ((d.device_exposure_start_date between feat.pre_window_start_dt and feat.pre_window_end_dt) 
             or (d.device_exposure_end_date between feat.pre_window_start_dt and feat.pre_window_end_dt) 
             or (d.device_exposure_start_date < feat.pre_window_start_dt and d.device_exposure_end_date > feat.pre_window_end_dt))

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.6027a85f-b97f-40ea-b125-92f65183484e"),
    Feature_table_builder=Input(rid="ri.foundry.main.dataset.ce7a93a0-4140-4fdb-b97d-fb78c0caf345"),
    device_filtered=Input(rid="ri.vector.main.execute.c4f0173a-942b-4c1e-b6b4-b1b7183e0d91")
)
select distinct d.person_id, d.device_concept_name, 1 as prepre_device_count
FROM device_filtered d
    JOIN 
    (SELECT person_id, pre_pre_window_start_dt, pre_window_start_dt FROM Feature_table_builder) feat
    ON d.person_id = feat.person_id 
       and ((d.device_exposure_start_date between feat.pre_pre_window_start_dt and feat.pre_window_start_dt) 
             or (d.device_exposure_end_date between feat.pre_pre_window_start_dt and feat.pre_window_start_dt) 
             or (d.device_exposure_start_date < feat.pre_pre_window_start_dt and d.device_exposure_end_date > feat.pre_window_start_dt))

