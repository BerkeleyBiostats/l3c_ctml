

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.ef013589-e3b1-42ca-918d-b01a3ac31567")
)
SELECT *, floor(1000*rand()) as fold_id, row_number() over (order by (select null)) as idx
FROM final_test_dat

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.f6c58600-69ab-4188-b779-a2a4495acc2c"),
    full_train_dat=Input(rid="ri.foundry.main.dataset.1ef41f96-7291-4a0b-9cd7-1d7e24a5a5d8")
)
SELECT *, floor(1000*rand()) as fold_id, row_number() over (order by (select null)) as idx
FROM full_train_dat

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.c54163b1-fbac-4236-8812-ec2cc6ce00fe"),
    test_dat=Input(rid="ri.foundry.main.dataset.148e16cb-3dcc-4dff-a4b4-345a6643cc09")
)
SELECT *, floor(1000*rand()) as fold_id, row_number() over (order by (select null)) as idx
FROM test_dat

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.a2069244-8353-4633-941f-87be403cdceb"),
    train_dat=Input(rid="ri.foundry.main.dataset.a3901c47-dd19-49f5-a18b-98c68e8cc651")
)
SELECT *, floor(1000*rand()) as fold_id, row_number() over (order by (select null)) as idx
FROM train_dat

