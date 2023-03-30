

@transform_pandas(
    Output(rid="ri.vector.main.execute.10148bd6-5cdc-4c88-831a-5d41fb3c43d4"),
    Feature_table_builder=Input(rid="ri.foundry.main.dataset.ce7a93a0-4140-4fdb-b97d-fb78c0caf345"),
    measurement_person=Input(rid="ri.vector.main.execute.b6af7e97-05b8-4768-9552-1e4046b18cf0")
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
    Output(rid="ri.vector.main.execute.425747f0-a16e-4568-ae64-e4c637311ea5"),
    covid_measurement=Input(rid="ri.vector.main.execute.10148bd6-5cdc-4c88-831a-5d41fb3c43d4"),
    post_measurement=Input(rid="ri.vector.main.execute.e981f20a-6b6d-48bd-973c-3b5a3ecc47a5"),
    pre_measurement=Input(rid="ri.vector.main.execute.938e04da-7ae1-43b6-a973-f3781c98edcf"),
    pre_pre_measurement=Input(rid="ri.vector.main.execute.88dcef06-2537-4c90-a39f-a42ffa4b461b")
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
    Output(rid="ri.foundry.main.dataset.b6344895-930e-4c2c-a065-2078d950abff"),
    four_windows_measure=Input(rid="ri.vector.main.execute.425747f0-a16e-4568-ae64-e4c637311ea5")
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
    Output(rid="ri.vector.main.execute.e981f20a-6b6d-48bd-973c-3b5a3ecc47a5"),
    Feature_table_builder=Input(rid="ri.foundry.main.dataset.ce7a93a0-4140-4fdb-b97d-fb78c0caf345"),
    measurement_person=Input(rid="ri.vector.main.execute.b6af7e97-05b8-4768-9552-1e4046b18cf0")
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
    Output(rid="ri.vector.main.execute.938e04da-7ae1-43b6-a973-f3781c98edcf"),
    Feature_table_builder=Input(rid="ri.foundry.main.dataset.ce7a93a0-4140-4fdb-b97d-fb78c0caf345"),
    measurement_person=Input(rid="ri.vector.main.execute.b6af7e97-05b8-4768-9552-1e4046b18cf0")
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
    Output(rid="ri.vector.main.execute.88dcef06-2537-4c90-a39f-a42ffa4b461b"),
    Feature_table_builder=Input(rid="ri.foundry.main.dataset.ce7a93a0-4140-4fdb-b97d-fb78c0caf345"),
    measurement_person=Input(rid="ri.vector.main.execute.b6af7e97-05b8-4768-9552-1e4046b18cf0")
)
-- find all conditions associated with patients in their pre-pre window
SELECT feat.person_id, m.measurement_concept_name, m.measurement_concept_id, 
max(m.harmonized_value_as_number) as max_measure, min(m.harmonized_value_as_number) as min_measure, 
avg(m.harmonized_value_as_number) as avg_measure
FROM Feature_table_builder feat 
JOIN 
measurement_person m ON feat.person_id = m.person_id and m.visit_date between feat.pre_pre_window_start_dt and feat.pre_window_start_dt
GROUP BY feat.person_id, m.measurement_concept_name, m.measurement_concept_id

