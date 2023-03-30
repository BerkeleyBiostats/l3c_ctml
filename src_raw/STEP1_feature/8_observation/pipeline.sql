

@transform_pandas(
    Output(rid="ri.vector.main.execute.bb5738c9-628e-4466-a2ce-cde2c411aef1"),
    Feature_table_builder=Input(rid="ri.foundry.main.dataset.ce7a93a0-4140-4fdb-b97d-fb78c0caf345"),
    observation=Input(rid="ri.foundry.main.dataset.f9d8b08e-3c9f-4292-b603-f1bfa4336516")
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
    Output(rid="ri.vector.main.execute.6b72ca05-314e-44a8-bf4d-b16482206e54"),
    obs_person=Input(rid="ri.vector.main.execute.bb5738c9-628e-4466-a2ce-cde2c411aef1")
)
SELECT * 
FROM obs_person op
WHERE op.observation_concept_name IN
(SELECT op.observation_concept_name
FROM obs_person op
GROUP BY observation_concept_name
HAVING COUNT(*) > 10) -- exclude sparse concepts

