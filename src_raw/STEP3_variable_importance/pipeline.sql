

@transform_pandas(
    Output(rid="ri.vector.main.execute.d0868612-0378-4999-9dd9-1da6ad4f010c"),
    Feature_table_all_patients_dx_drug=Input(rid="ri.foundry.main.dataset.1ef41f96-7291-4a0b-9cd7-1d7e24a5a5d8")
)
SELECT *, floor(1000*rand()) as fold_id, row_number() over (order by (select null)) as idx
FROM Feature_table_all_patients_dx_drug

