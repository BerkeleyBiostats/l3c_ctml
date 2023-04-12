from src.ml_functions import *
from pyspark.sql.functions import monotonically_increasing_id 

#--------------------#
#    Import Data
#--------------------#

# # Simulate Data
# from sklearn.datasets import make_classification
# X, y = make_classification(
#     n_samples=2000,  # row number
#     n_features=4, # feature numbers
#     n_informative=3, # The number of informative features
#     n_redundant = 1, # The number of redundant features
#     n_repeated = 0, # The number of duplicated features
#     n_classes = 2, # The number of classes 
#     n_clusters_per_class=1,#The number of clusters per class
#     random_state = 42 # random seed 
# )

# df_np = np.c_[X,y]
# df_pd = pd.DataFrame(df_np, columns=['x%0.0f' % i for i in range(X.shape[1]) ] + ['long_covid'])
# df = spark.createDataFrame(df_np.tolist(), ['x%0.0f' % i for i in range(X.shape[1]) ] + ['long_covid'])

# TBD Import Synthetic Data
df = pd.read_csv('data.csv')
df = df.select("*").withColumn("idx", monotonically_increasing_id()+1)

#--------------------------------#
#    Train SL (on training data)
#--------------------------------#
# define sl library
sl_library = {'lrnr_logistic': lrnr_logistic, 
    'lrnr_lasso1': lrnr_lasso1,
    'lrnr_rf5': lrnr_rf5,
    'lrnr_gb20': lrnr_gb20}

# train sl
df_res = train_sl(df)

# check sl summary
df_res.show()


#--------------------------------#
#    SL predict (on testing data)
#--------------------------------#
# TBD Import Synthetic Test Data
df_test = pd.read_csv('data.csv')
df_preds = targeted_ml_team_predictions(train_sl = df_res, 
    analytic_full_train = df, 
    analytic_final_test = df_test)

# check sl summary
df_preds.show()


#-------------------------#
#    Make Shapley Plots
#-------------------------#
plots_simple(df)


