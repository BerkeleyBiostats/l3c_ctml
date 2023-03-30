import functools

# make versions of the transform/input/output funcs that we can use to extract
# data from the decorators
def Input(rid):
    return(rid)

def Output(rid):
    return(rid)

# TODO: find different spot for this transform to scrape the rid/name mapping
def transform_pandas(Output, **trans_kwargs):
    def decorator_transform_pandas(func):
        @functools.wraps(func)
        def wrapper_transform_pandas(*args, **kwargs):
            all_outputs.append(Output)
            new_args = [(arg, val) for arg,val in trans_kwargs.items()]
            all_args.extend(new_args)
        return wrapper_transform_pandas
    return decorator_transform_pandas


@transform_pandas(
    Output(rid="ri.foundry.main.dataset.ce7a93a0-4140-4fdb-b97d-fb78c0caf345"),
    Feature_Table_Builder_v0=Input(rid="ri.vector.main.execute.e26f3947-ea85-4de9-b662-4048a52ec048"),
    tot_icu_days_calc=Input(rid="ri.vector.main.execute.e8f9f7e0-1c42-44d6-8fcd-20cc54971623"),
    tot_ip_days_calc=Input(rid="ri.vector.main.execute.fe1ce00c-f84c-4fc6-b1bb-d3a268301ade")
)
def run_sql():
    sql = """
    SELECT feat.*, 
    (nvl(tot_ip.post_tot_ip_days, 0)/feat.tot_post_days) as post_ip_visit_ratio, 
    (nvl(tot_ip.covid_tot_ip_days, 0)/feat.tot_covid_days) as covid_ip_visit_ratio, 
    (nvl(tot_icu.post_tot_icu_days, 0)/feat.tot_post_days) as post_icu_visit_ratio, 
    (nvl(tot_icu.covid_tot_icu_days, 0)/feat.tot_covid_days) as covid_icu_visit_ratio
    FROM Feature_Table_Builder_v0 feat
    LEFT JOIN tot_ip_days_calc tot_ip ON feat.person_id = tot_ip.person_id
    LEFT JOIN tot_icu_days_calc tot_icu ON feat.person_id = tot_icu.person_id
    """

run_sql()