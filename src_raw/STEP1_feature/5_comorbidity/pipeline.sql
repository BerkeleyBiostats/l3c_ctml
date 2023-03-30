

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.2513e4c2-bba0-4067-843f-dec2dfa2b858"),
    Feature_table_builder=Input(rid="ri.foundry.main.dataset.ce7a93a0-4140-4fdb-b97d-fb78c0caf345"),
    high_level_condition_occur=Input(rid="ri.vector.main.execute.272bff24-17cd-4c3c-acc1-6972bc51deea")
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
    Output(rid="ri.vector.main.execute.272bff24-17cd-4c3c-acc1-6972bc51deea"),
    Feature_table_builder=Input(rid="ri.foundry.main.dataset.ce7a93a0-4140-4fdb-b97d-fb78c0caf345"),
    condition_occurrence=Input(rid="ri.foundry.main.dataset.2f496793-6a4e-4bf4-b0fc-596b277fb7e2")
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

