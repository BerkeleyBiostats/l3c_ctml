#--------------------#
#     load modules
#--------------------#
import pandas as pd
import numpy as np
# import shap
import matplotlib.pyplot as plt
import matplotlib.colors as mcolors
from sklearn import metrics
from scipy.optimize import minimize
from scipy.optimize import Bounds

from sklearn.model_selection import KFold
from sklearn.model_selection import StratifiedKFold as SKFold

from pyspark.ml.feature import VectorAssembler, ChiSqSelector
from pyspark.ml.regression import LinearRegression
from pyspark.ml import Pipeline
from pyspark.mllib.evaluation import BinaryClassificationMetrics
from pyspark.mllib.evaluation import RegressionMetrics
from pyspark.sql.functions import pow, col, mean, lit, udf
from pyspark.sql import functions as F
from pyspark.sql import SparkSession

from pyspark.sql.types import DoubleType, StringType, BooleanType, IntegerType
from pyspark.sql.types import StructType, StructField

from pyspark.ml.functions import vector_to_array
from pyspark.ml.classification import LogisticRegression as Lrnr_logistic
from pyspark.ml.classification import RandomForestClassifier as Lrnr_rf
from pyspark.ml.classification import GBTClassifier as Lrnr_gb
from pyspark.ml.classification import MultilayerPerceptronClassifier as Lrnr_mpc
from pyspark.ml.classification import LinearSVC as Lrnr_svc
from pyspark.ml.classification import NaiveBayes as Lrnr_nb
from pyspark.ml.classification import FMClassifier as Lrnr_fmc
from pyspark.ml.classification import OneVsRest as Lrnr_onevsrest

from pyspark.ml.evaluation import BinaryClassificationEvaluator
from pyspark.ml.tuning import ParamGridBuilder, CrossValidator

# from sl3py.learners import *
spark = SparkSession.builder.getOrCreate()

#--------------------#
#     SL config
#--------------------#
# define lr lib
lrnr_logistic = Lrnr_logistic(labelCol='long_covid') # (auc ~0.86)
lrnr_lasso1 = Lrnr_logistic(labelCol='long_covid', regParam = 0.01, elasticNetParam = 1) # (auc ~0.87)
lrnr_lasso2 = Lrnr_logistic(labelCol='long_covid', regParam = 0.1, elasticNetParam = 1)
lrnr_rf = Lrnr_rf(labelCol='long_covid') # (auc ~0.88)
lrnr_rf3 = Lrnr_rf(labelCol='long_covid', maxDepth=3) # (auc ~0.88)
lrnr_rf7 = Lrnr_rf(labelCol='long_covid', maxDepth=7) # (auc ~0.88)

lrnr_rf_log2 = Lrnr_rf(labelCol='long_covid', featureSubsetStrategy = 'log2')
lrnr_rf_sqrt = Lrnr_rf(labelCol='long_covid', featureSubsetStrategy = 'sqrt')
lrnr_rf_onethird = Lrnr_rf(labelCol='long_covid', featureSubsetStrategy = 'onethird')
lrnr_rf_half = Lrnr_rf(labelCol='long_covid', featureSubsetStrategy = '0.5')
lrnr_rf_all = Lrnr_rf(labelCol='long_covid', featureSubsetStrategy = 'all')

lrnr_gb20 = Lrnr_gb(labelCol='long_covid', maxIter = 20)
lrnr_gb50 = Lrnr_gb(labelCol='long_covid', maxIter = 50) # (auc ~0.89)
lrnr_gb100 = Lrnr_gb(labelCol='long_covid', maxIter = 100) # (auc ~0.90)
lrnr_gb150 = Lrnr_gb(labelCol='long_covid', maxIter = 150)
lrnr_gb200 = Lrnr_gb(labelCol='long_covid', maxIter = 200) # (auc ~0.90)
lrnr_gb210 = Lrnr_gb(labelCol='long_covid', maxIter = 210)
lrnr_gb220 = Lrnr_gb(labelCol='long_covid', maxIter = 220)
lrnr_gb250 = Lrnr_gb(labelCol='long_covid', maxIter = 250)
lrnr_gb275 = Lrnr_gb(labelCol='long_covid', maxIter = 275)
lrnr_gb300 = Lrnr_gb(labelCol='long_covid', maxIter = 300)

lrnr_rf2 = Lrnr_rf(labelCol='long_covid', maxDepth=2, featureSubsetStrategy = 'onethird')
lrnr_rf4 = Lrnr_rf(labelCol='long_covid', maxDepth=4, featureSubsetStrategy = 'onethird')
lrnr_rf5 = Lrnr_rf(labelCol='long_covid', maxDepth=5, featureSubsetStrategy = 'onethird')
lrnr_rf6 = Lrnr_rf(labelCol='long_covid', maxDepth=6, featureSubsetStrategy = 'onethird')
lrnr_rf8 = Lrnr_rf(labelCol='long_covid', maxDepth=8, featureSubsetStrategy = 'onethird')
lrnr_rf10 = Lrnr_rf(labelCol='long_covid', maxDepth=10, featureSubsetStrategy = 'onethird')

lrnr_gb001 = Lrnr_gb(labelCol='long_covid', maxIter = 150, stepSize=0.01)
lrnr_gb005 = Lrnr_gb(labelCol='long_covid', maxIter = 150, stepSize=0.05)
lrnr_gb01 = Lrnr_gb(labelCol='long_covid', maxIter = 150, stepSize=0.1)
lrnr_gb015 = Lrnr_gb(labelCol='long_covid', maxIter = 150, stepSize=0.15)
lrnr_gb02 = Lrnr_gb(labelCol='long_covid', maxIter = 150, stepSize=0.2)

lrnr_gb2 = Lrnr_gb(labelCol='long_covid', maxIter = 150, maxDepth=2)
lrnr_gb3 = Lrnr_gb(labelCol='long_covid', maxIter = 150, maxDepth=3)
lrnr_gb4 = Lrnr_gb(labelCol='long_covid', maxIter = 150, maxDepth=4)
lrnr_gb5 = Lrnr_gb(labelCol='long_covid', maxIter = 150, maxDepth=5)
lrnr_gb6 = Lrnr_gb(labelCol='long_covid', maxIter = 150, maxDepth=6)
lrnr_gb7 = Lrnr_gb(labelCol='long_covid', maxIter = 150, maxDepth=7)
lrnr_gb9 = Lrnr_gb(labelCol='long_covid', maxIter = 150, maxDepth=9)
lrnr_gb11 = Lrnr_gb(labelCol='long_covid', maxIter = 150, maxDepth=11)

# lrnr_nb = Lrnr_nb(labelCol='long_covid') # poor perf (auc ~0.64)
lrnr_mpc = Lrnr_mpc(labelCol='long_covid', layers=[764,200,50,2], seed=1) # (auc ~0.85)
lrnr_mpc3 = Lrnr_mpc(labelCol='long_covid', layers=[764,512,2], seed=1)
lrnr_mpc4 = Lrnr_mpc(labelCol='long_covid', layers=[764,512,256,2], seed=1)
lrnr_mpc5 = Lrnr_mpc(labelCol='long_covid', layers=[764,512,256,128,2], seed=1)
lrnr_mpc6 = Lrnr_mpc(labelCol='long_covid', layers=[764,512,256,128,64,2], seed=1)
lrnr_mpc7 = Lrnr_mpc(labelCol='long_covid', layers=[764,512,256,128,64,32,2], seed=1)

lrnr_fmc = Lrnr_fmc(labelCol='long_covid', stepSize=0.001) # poor perf (auc ~0.74)
lrnr_svc = Lrnr_svc(labelCol='long_covid')
lrnr_onevsrest = Lrnr_onevsrest(classifier=lrnr_svc, labelCol='long_covid')

# define sl library
sl_library = {'lrnr_logistic': lrnr_logistic, 
    'lrnr_lasso1': lrnr_lasso1,
    'lrnr_rf5': lrnr_rf5,
    'lrnr_gb200': lrnr_gb200}

# square error
def square_error(y_hat, y):
    return (y_hat - y)**2

# binomial log-likelihood 
def binomial_loglik_error(y_hat, y):
    return - y*F.log(y_hat) - (1 - y)*F.log(1-y_hat)

# loss_auc (numpy)
def loss_auc(y_hat, y):
    fpr, tpr, _ = metrics.roc_curve(y, y_hat, pos_label = 1)
    auc_neg = -1 * metrics.auc(fpr, tpr)
    return auc_neg
# auc (pyspark)
auc_pyspark = BinaryClassificationEvaluator(labelCol='long_covid')

# loss_entropy
def loss_entropy(y_hat, y):
    loss = mean(binomial_loglik_error(y_hat, y))
    return loss

# loss_square_error
def loss_square_error(y_hat, y):
    loss = mean(square_error(y_hat, y))
    return loss

#--------------------#
#     archive
#--------------------#

# class CurveMetrics(BinaryClassificationMetrics):
#     def __init__(self, *args):
#         super(CurveMetrics, self).__init__(*args)

#     def _to_list(self, rdd):
#         points = []
#         # Note this collect could be inefficient for large datasets 
#         # considering there may be one probability per datapoint (at most)
#         # The Scala version takes a numBins parameter, 
#         # but it doesn't seem possible to pass this from Python to Java
#         for row in rdd.collect():
#             # Results are returned as type scala.Tuple2, 
#             # which doesn't appear to have a py4j mapping
#             points += [(float(row._1()), float(row._2()))]
#         return points

#     def get_curve(self, method):
#         rdd = getattr(self._java_model, method)().toJavaRDD()
#         return self._to_list(rdd)

# def loss_auc(predictionAndLabels): # (pyspark)
#     metrics = BinaryClassificationMetrics(predictionAndLabels)
#     auc_neg = -1 * metrics.areaUnderROC
#     return auc_neg

# helper 
# def extract_prob(v):
#     try:
#         return float(v[1]) 
#     except ValueError:
#         return None
# get_prob = udf(extract_prob, DoubleType())

# # create a parameter grid
# params_rf = ParamGridBuilder() \
#            .addGrid(lrnr_rf.featureSubsetStrategy, ['all', 'onethird', 'sqrt']) \
#            .addGrid(lrnr_rf.maxDepth, [3, 5, 7]) \
#            .build()

# params_gb = ParamGridBuilder() \
#            .addGrid(lrnr_gb.maxIter, [50, 100, 200]) \
#            .addGrid(lrnr_gb.maxDepth, [3, 5, 7]) \
#            .build()

# # create a binary classification evaluator
# evaluator_binary = BinaryClassificationEvaluator(labelCol='long_covid')

# # create a cross-validator
# cv_rf = CrossValidator(estimator = lrnr_rf, 
#                        estimatorParamMaps = params_rf, 
#                        evaluator = evaluator_binary, 
#                        numFolds = 5)

# cv_gb = CrossValidator(estimator = Lrnr_gb, 
#                        estimatorParamMaps = params_gb, 
#                        evaluator = evaluator_binary, 
#                        numFolds = 5)

# sl_library = {'lrnr_logistic': lrnr_logistic, 'lrnr_gb50': lrnr_gb50}

# sl_library = {'lrnr_gb20': lrnr_gb20, 
#     'lrnr_gb50': lrnr_gb50,
#     'lrnr_gb100': lrnr_gb100,
#     'lrnr_gb150': lrnr_gb150,
#     'lrnr_gb200': lrnr_gb200}

# sl_library = {'lrnr_gb001': lrnr_gb001,
#     'lrnr_gb005': lrnr_gb005,
#     'lrnr_gb01': lrnr_gb01,
#     'lrnr_gb015': lrnr_gb015,
#     'lrnr_gb02': lrnr_gb02}

# sl_library = {'lrnr_gb2': lrnr_gb2, 
#     'lrnr_gb3': lrnr_gb3,
#     'lrnr_gb4': lrnr_gb4,
#     'lrnr_gb5': lrnr_gb5,
#     'lrnr_gb6': lrnr_gb6,
#     'lrnr_gb7': lrnr_gb7}

# sl_library = {'lrnr_rf_log2': lrnr_rf_log2, 
#     'lrnr_rf_sqrt': lrnr_rf_sqrt,
#     'lrnr_rf_onethird': lrnr_rf_onethird,
#     'lrnr_rf_half': lrnr_rf_half,
#     'lrnr_rf_all': lrnr_rf_all}

# sl_library = {'lrnr_rf2': lrnr_rf2, 
#     'lrnr_rf4': lrnr_rf4,
#     'lrnr_rf5': lrnr_rf5,
#     'lrnr_rf6': lrnr_rf6,
#     'lrnr_rf8': lrnr_rf8,
#     'lrnr_rf10': lrnr_rf10}

# sl_library = {'lrnr_mpc3': lrnr_mpc3, 
#     'lrnr_mpc4': lrnr_mpc4,
#     'lrnr_mpc5': lrnr_mpc5,
#     'lrnr_mpc6': lrnr_mpc6,
#     'lrnr_mpc7': lrnr_mpc7}

# sl_library = {'lrnr_logistic': lrnr_logistic, 
#     'lrnr_rf8': lrnr_rf8,
#     'lrnr_lasso1': lrnr_lasso1,
#     'lrnr_gb275': lrnr_gb275}

# sl_library = {'lrnr_logistic': lrnr_logistic, 
#     'lrnr_lasso1': lrnr_lasso1,
#     'lrnr_rf_tuned': lrnr_rf_tuned,
#     'lrnr_gb_tuned': lrnr_gb_tuned}

# sl_library = {'lrnr_rf5': lrnr_rf5, 
#     'lrnr_rf7': lrnr_rf7,
#     'lrnr_gb50': lrnr_gb50,
#     'lrnr_gb200': lrnr_gb200}

@transform_pandas(
    Output(rid="ri.vector.main.execute.aa1442e2-3bc9-424a-999c-f33ae6cf38fe"),
    analytic_test_dat=Input(rid="ri.foundry.main.dataset.c54163b1-fbac-4236-8812-ec2cc6ce00fe"),
    analytic_train_dat=Input(rid="ri.foundry.main.dataset.a2069244-8353-4633-941f-87be403cdceb"),
    train_sl_sub=Input(rid="ri.foundry.main.dataset.5d4c0164-c0b6-4266-b5e9-7cf12b624bcc")
)
def eval_sub_fit(train_sl_sub, analytic_train_dat, analytic_test_dat):
    # get lrnr weights
    df_metalearner =  train_sl_sub.toPandas()
    wts = df_metalearner['weights'].to_numpy()[:-1]
    lrnr_names = df_metalearner['learner'].to_numpy()[:-1]

    # fit each learner on full data, get predictions on test data
    df_t = analytic_train_dat
    df_v = analytic_test_dat
    preds_v = df_v.select('idx', 'person_id', 'long_covid')
    bad_cols = ['long_covid', 'person_id', 'fold_id', 'idx', 'in_train']
    covariates = [col for col in df_t.columns if col not in bad_cols]
    # assemble features
    assembler = VectorAssembler(
        inputCols = covariates,
        outputCol = "features"
        )
    # loop over learners
    for lr in lrnr_names:
        # print("fit ", lr)
        # create pipeline
        pipeline = Pipeline(stages=[assembler, sl_library[lr]])
        # fit on training set
        pipeline = pipeline.fit(df_t)
        # predict on validation set
        preds = pipeline.transform(df_v)
        preds = preds.withColumn('prob_hat_' + lr, 
            vector_to_array('probability')[1])
        # collect results
        just_preds = preds.select('idx','long_covid','prob_hat_' + lr)
        preds_v = preds_v.join(just_preds, ['idx', 'long_covid'], "left")
    
    # get auc and preds for ensemble sl
    df_pd = preds_v.toPandas()
    person_id = df_pd['person_id'].to_numpy()
    # get y
    y = df_pd['long_covid'].to_numpy()
    # get learner preds cols
    preds_cols = [c for c in df_pd if c.startswith('prob_hat_')]
    preds = df_pd[preds_cols]
    # weighted sum over all learner preds
    weighted_preds = np.sum(wts * preds.to_numpy(), axis = 1)
    loss = loss_auc(weighted_preds, y)
    print("auc of the ensemble sl: ", -loss)

    # output predictions
    # predctions = (weighted_preds > 0.5).astype(int)
    # df_res_pd = pd.DataFrame({'person_id': person_id, 'predictions': predctions})
    # df_res = spark.createDataFrame(df_res_pd)
    return None

    

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.081485ed-7e6f-4722-aa9a-b3bae2fc76a9"),
    analytic_final_test=Input(rid="ri.foundry.main.dataset.ef013589-e3b1-42ca-918d-b01a3ac31567"),
    analytic_full_train=Input(rid="ri.foundry.main.dataset.f6c58600-69ab-4188-b779-a2a4495acc2c"),
    train_sl=Input(rid="ri.foundry.main.dataset.f272d26c-a42b-41d2-b2db-05a3c5da5f13")
)
def targeted_ml_team_predictions(train_sl, analytic_full_train, analytic_final_test):
    # get lrnr weights
    df_metalearner =  train_sl.toPandas()
    wts = df_metalearner['weights'].to_numpy()[:-1]
    lrnr_names = df_metalearner['learner'].to_numpy()[:-1]
    # fit each learner on full data, get predictions on test data
    df_t = analytic_full_train
    df_v = analytic_final_test
    preds_v = df_v.select('idx', 'person_id')
    bad_cols = ['long_covid', 'person_id', 'fold_id', 'idx', 'in_train']
    covariates = [col for col in df_t.columns if col not in bad_cols]
    # assemble features
    assembler = VectorAssembler(
        inputCols = covariates,
        outputCol = "features"
        )
    # loop over learners
    for lr in lrnr_names:
        # print("fit ", lr)
        # create pipeline
        pipeline = Pipeline(stages=[assembler, sl_library[lr]])
        # fit on training set
        pipeline = pipeline.fit(df_t)
        # predict on validation set
        preds = pipeline.transform(df_v)
        preds = preds.withColumn('prob_hat_' + lr, 
            vector_to_array('probability')[1])
        # collect results
        just_preds = preds.select('idx','prob_hat_' + lr)
        preds_v = preds_v.join(just_preds, ['idx'], "left")
    
    # get auc and preds for ensemble sl
    df_pd = preds_v.toPandas()
    person_id = df_pd['person_id'].to_numpy()
    # get learner preds cols
    preds_cols = [c for c in df_pd if c.startswith('prob_hat_')]
    preds = df_pd[preds_cols]
    # weighted sum over all learner preds
    weighted_preds = np.sum(wts * preds.to_numpy(), axis = 1)
    # output predictions
    # predctions = (weighted_preds > 0.5).astype(int)
    df_res_pd = pd.DataFrame({'person_id': person_id, 'outcome_likelihood': weighted_preds})
    df_res = spark.createDataFrame(df_res_pd)
    return df_res

    

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.148e16cb-3dcc-4dff-a4b4-345a6643cc09"),
    full_train_dat=Input(rid="ri.foundry.main.dataset.1ef41f96-7291-4a0b-9cd7-1d7e24a5a5d8")
)
def test_dat(full_train_dat):
    df = full_train_dat
    _, df_test = df.randomSplit([0.9, 0.1], seed=123)
    return df_test
    

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.a3901c47-dd19-49f5-a18b-98c68e8cc651"),
    full_train_dat=Input(rid="ri.foundry.main.dataset.1ef41f96-7291-4a0b-9cd7-1d7e24a5a5d8")
)
def train_dat(full_train_dat):
    df = full_train_dat
    df_train, _ = df.randomSplit([0.9, 0.1], seed=123)
    return df_train

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.f272d26c-a42b-41d2-b2db-05a3c5da5f13"),
    analytic_full_train=Input(rid="ri.foundry.main.dataset.f6c58600-69ab-4188-b779-a2a4495acc2c")
)
def train_sl(analytic_full_train):
    analytic_full = analytic_full_train
    # base fit
    train_res = cv_train(analytic_full)
    # meta fit
    meta_res = meta_train(train_res)
    return meta_res  

# (stratified) cv_train
# Input: full data, 
#        sl_library, 
#        num of cv folds, 
#        var name for stratification
# Output: a dataframe wirh idx, outcome and cv-preds of each learner
def cv_train(df, sl_library = sl_library, k = 10, strata_var = 'long_covid'):    
    n = df.count()
    idx = np.arange(1, n+1)

    if strata_var is None:
        # direct fold split
        folds = KFold(n_splits = k)
        folds_idx = folds.split(idx)
    else:
        # generate stratified k folds
        # make sure there are at least k obs in each strata
        s = np.array(df.select(strata_var).collect())
        folds = SKFold(n_splits = k)
        folds_idx = folds.split(idx, s)

    # specify features
    # todo: get list of covariates from data team
    # bad_cols = ['long_covid', 'person_id', 'fold_id', 'idx',
    #             'in_train', 'total_pre_dx', 'total_post_dx']
    bad_cols = ['long_covid', 'person_id', 'fold_id', 'idx', 'in_train']
    covariates = [col for col in df.columns if col not in bad_cols]

    # assemble features
    assembler = VectorAssembler(
        inputCols = covariates,
        outputCol = "features"
        )

    # create empty result container
    schema_input1 = [StructField('idx', IntegerType(), True),
        StructField('long_covid', DoubleType(), True)]
    schema_input2 = [StructField('prob_hat_' + lr, DoubleType(), True) for lr in sl_library]
    schema_input = schema_input1 + schema_input2
    schema = StructType(schema_input)
    emptyRDD = spark.sparkContext.emptyRDD()
    preds_v_all = spark.createDataFrame(emptyRDD, schema)

    # model fitting
    fold_id = 1
    # loop over k folds
    for idx_t, _ in folds_idx:
        # make a indicator dataset and merge
        in_train = np.isin(idx, idx_t)
        df_pd = pd.DataFrame({'idx': idx, 'in_train': in_train})
        fold_df = spark.createDataFrame(df_pd)
        # merge indicator dataset
        df_all = df.join(fold_df, ['idx'], "inner")
        # pull out training/validation set
        df_t = df_all.filter("in_train == TRUE")
        df_v = df_all.filter("in_train == FALSE")

        preds_v = df_v.select('idx', 'long_covid')
        # loop over learners
        for lr in sl_library:
            # create pipeline
            pipeline = Pipeline(stages=[assembler, sl_library[lr]])
            # fit on training set
            pipeline = pipeline.fit(df_t)
            # predict on validation set
            preds = pipeline.transform(df_v)
            preds = preds.withColumn('prob_hat_' + lr, 
                vector_to_array('probability')[1])
            # collect results
            just_preds = preds.select('idx','long_covid','prob_hat_' + lr)
            preds_v = preds_v.join(just_preds, ['idx', 'long_covid'], "left")
        preds_v_all = preds_v_all.union(preds_v)
        fold_id += 1
    return preds_v_all

# meta_train 
# Input: a dataframe with idx, outcome and cv preds of each learner
# Output: a dataframe with learner names, weights and cv risks
def meta_train(df, meta_learner = 'auc_powell', loss_f = loss_auc, normalize = True):
    # convert to pandas for now
    # since no handy optimization algrithm found in pyspark
    df_pd = df.toPandas()
    lrnr_names = list(sl_library.keys())
    n_l = len(lrnr_names)
    lrnr_weights = [0]*n_l
    lrnr_risks = []

    # calc cv risk for each learner
    for lr in sl_library:
        y_hat = df_pd['prob_hat_' + lr].to_numpy()
        y = df_pd['long_covid'].to_numpy()
        cv_risk = loss_f(y_hat, y)
        lrnr_risks.append(cv_risk)

    # discrete sl
    if meta_learner == 'discrete':
        idx_opt = np.argmin(lrnr_risks)
        lrnr_weights[idx_opt] = 1
        sl_risk = lrnr_risks[idx_opt]
    
    # ensemble sl
    elif meta_learner == 'auc_lbfgsb': # not work, no update
        # equal initial weighting
        x0 = [1/n_l]*n_l
        bds = Bounds([0]*n_l, [1]*n_l)
        cons = ({'type': 'eq', 'fun': lambda x:  sum(x) - 1})
        
        opt_res = minimize(meta_objective, x0, 
            args=(df_pd, loss_f), method='L-BFGS-B', 
            bounds=bds, constraints=cons, options={'disp': True})

        # update learner weights 
        lrnr_weights = opt_res.x.tolist()
        print(opt_res.message)
        sl_risk = meta_objective(lrnr_weights, df_pd, loss_f)

    elif meta_learner == 'auc_bfgs': # seems work, but very few updates
        # equal initial weighting
        x0 = [1/n_l]*n_l
        opt_res = minimize(meta_objective, x0, 
            args=(df_pd, loss_f), method='BFGS', options={'disp': True})
        # update learner weights
        lrnr_weights = opt_res.x.tolist()
        print(opt_res.message)
        print(lrnr_weights)
        sl_risk = meta_objective(lrnr_weights, df_pd, loss_f)

    elif meta_learner == 'auc_slsqp': # seems work, but very few iterations
        # equal initial weighting
        x0 = [1/n_l]*n_l
        bds = Bounds([0]*n_l, [1]*n_l)
        cons = ({'type': 'eq', 'fun': lambda x:  sum(x) - 1})
        opt_res = minimize(meta_objective, x0, 
            args=(df_pd, loss_f), method='SLSQP', 
            bounds=bds, constraints=cons, options={'disp': True})
        # update learner weights
        lrnr_weights = opt_res.x.tolist()
        print(opt_res.message)
        sl_risk = meta_objective(lrnr_weights, df_pd, loss_f)

    elif meta_learner == 'auc_nm': # works (bounds not work)
        # equal initial weighting
        x0 = [1/n_l]*n_l
        bds = ((0,1),) * n_l
        opt_res = minimize(meta_objective, x0, 
            args=(df_pd, loss_f), method='Nelder-Mead', 
            bounds=bds, tol=1e-6, options={'disp': True})
        # update learner weights
        lrnr_weights = opt_res.x.tolist()
        print(opt_res.message)
        # print('lalala: ', lrnr_weights)
        sl_risk = meta_objective(lrnr_weights, df_pd, loss_f)
    
    elif meta_learner == 'auc_powell': # works
        # equal initial weighting
        x0 = [1/n_l]*n_l
        # bds = Bounds([0]*n_l, [1]*n_l)
        bds = ((0,1),) * n_l
        opt_res = minimize(meta_objective, x0, 
            args=(df_pd, loss_f), method='Powell', 
            bounds=bds, tol=1e-6, options={'disp': True})
        # update learner weights
        lrnr_weights = opt_res.x.tolist()
        print(opt_res.message)
        print('lalala: ', lrnr_weights)
        sl_risk = meta_objective(lrnr_weights, df_pd, loss_f)
    
    elif meta_learner == 'auc_tnc': # not work, no update
        # equal initial weighting
        x0 = [1/n_l]*n_l
        bds = ((0,1),) * n_l
        opt_res = minimize(meta_objective, x0, 
            args=(df_pd, loss_f), method='TNC', 
            bounds=bds, tol=1e-6, options={'disp': True})
        # update learner weights
        lrnr_weights = opt_res.x.tolist()
        print(opt_res.message)
        print('lalala: ', lrnr_weights)
        sl_risk = meta_objective(lrnr_weights, df_pd, loss_f)

    # add sl info
    lrnr_names.append('lrnr_sl') 
    if normalize and min(lrnr_weights) >= 0: 
        print('normalizing weights')
        lrnr_weights = [w/sum(lrnr_weights) for w in lrnr_weights]
    lrnr_weights.append(sum(lrnr_weights)) 
    lrnr_risks.append(sl_risk) 

    # output
    df_res = pd.DataFrame({'learner': lrnr_names, 
        'weights': lrnr_weights, 'cv_risk': lrnr_risks})
    df_res_sp = spark.createDataFrame(df_res)
    return df_res_sp

# Input: initial values of learner weights
#        a dataframe with idx, outcome and cv preds of each learner
#        a loss function
def meta_objective(x, df_pd, loss_f):
    # get y
    y = df_pd['long_covid'].to_numpy()
    # get learner preds cols
    preds_cols = [c for c in df_pd if c.startswith('prob_hat_')]
    preds = df_pd[preds_cols]
    # weighted sum over all learner preds
    weighted_preds = np.sum(x * preds.to_numpy(), axis = 1)
    loss = loss_f(weighted_preds, y)
    # add penalty if weights are negative
    loss += penalty_bds(x)
    return loss

# penalize negative weights
def penalty_bds(x):
    res = int(min(x) < 0) * 10
    return res

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.5d4c0164-c0b6-4266-b5e9-7cf12b624bcc"),
    analytic_train_dat=Input(rid="ri.foundry.main.dataset.a2069244-8353-4633-941f-87be403cdceb")
)
def train_sl_sub(analytic_train_dat):
    # base fit
    train_res = cv_train(analytic_train_dat)
    # meta fit
    meta_res = meta_train(train_res)
    return meta_res  

# (stratified) cv_train
# Input: full data, 
#        sl_library, 
#        num of cv folds, 
#        var name for stratification
# Output: a dataframe wirh idx, outcome and cv-preds of each learner
def cv_train(df, sl_library = sl_library, k = 10, strata_var = 'long_covid'):    
    n = df.count()
    idx = np.arange(1, n+1)

    if strata_var is None:
        # direct fold split
        folds = KFold(n_splits = k)
        folds_idx = folds.split(idx)
    else:
        # generate stratified k folds
        # make sure there are at least k obs in each strata
        s = np.array(df.select(strata_var).collect())
        folds = SKFold(n_splits = k)
        folds_idx = folds.split(idx, s)

    # specify features
    # todo: get list of covariates from data team
    # bad_cols = ['long_covid', 'person_id', 'fold_id', 'idx',
    #             'in_train', 'total_pre_dx', 'total_post_dx']
    bad_cols = ['long_covid', 'person_id', 'fold_id', 'idx', 'in_train']
    covariates = [col for col in df.columns if col not in bad_cols]

    # assemble features
    assembler = VectorAssembler(
        inputCols = covariates,
        outputCol = "features"
        )

    # create empty result container
    schema_input1 = [StructField('idx', IntegerType(), True),
        StructField('long_covid', DoubleType(), True)]
    schema_input2 = [StructField('prob_hat_' + lr, DoubleType(), True) for lr in sl_library]
    schema_input = schema_input1 + schema_input2
    schema = StructType(schema_input)
    emptyRDD = spark.sparkContext.emptyRDD()
    preds_v_all = spark.createDataFrame(emptyRDD, schema)

    # model fitting
    fold_id = 1
    # loop over k folds
    for idx_t, _ in folds_idx:
        # make a indicator dataset and merge
        in_train = np.isin(idx, idx_t)
        df_pd = pd.DataFrame({'idx': idx, 'in_train': in_train})
        fold_df = spark.createDataFrame(df_pd)
        # merge indicator dataset
        df_all = df.join(fold_df, ['idx'], "inner")
        # pull out training/validation set
        df_t = df_all.filter("in_train == TRUE")
        df_v = df_all.filter("in_train == FALSE")

        preds_v = df_v.select('idx', 'long_covid')
        # loop over learners
        for lr in sl_library:
            # create pipeline
            pipeline = Pipeline(stages=[assembler, sl_library[lr]])
            # fit on training set
            pipeline = pipeline.fit(df_t)
            # predict on validation set
            preds = pipeline.transform(df_v)
            preds = preds.withColumn('prob_hat_' + lr, 
                vector_to_array('probability')[1])
            # collect results
            just_preds = preds.select('idx','long_covid','prob_hat_' + lr)
            preds_v = preds_v.join(just_preds, ['idx', 'long_covid'], "left")
        preds_v_all = preds_v_all.union(preds_v)
        fold_id += 1
    return preds_v_all

# meta_train 
# Input: a dataframe with idx, outcome and cv preds of each learner
# Output: a dataframe with learner names, weights and cv risks
def meta_train(df, meta_learner = 'auc_powell', loss_f = loss_auc, normalize = True):
    # convert to pandas for now
    # since no handy optimization algrithm found in pyspark
    df_pd = df.toPandas()
    lrnr_names = list(sl_library.keys())
    n_l = len(lrnr_names)
    lrnr_weights = [0]*n_l
    lrnr_risks = []

    # calc cv risk for each learner
    for lr in sl_library:
        y_hat = df_pd['prob_hat_' + lr].to_numpy()
        y = df_pd['long_covid'].to_numpy()
        cv_risk = loss_f(y_hat, y)
        lrnr_risks.append(cv_risk)

    # discrete sl
    if meta_learner == 'discrete':
        idx_opt = np.argmin(lrnr_risks)
        lrnr_weights[idx_opt] = 1
        sl_risk = lrnr_risks[idx_opt]
    
    # ensemble sl
    elif meta_learner == 'auc_lbfgsb': # not work, no update
        # equal initial weighting
        x0 = [1/n_l]*n_l
        bds = Bounds([0]*n_l, [1]*n_l)
        cons = ({'type': 'eq', 'fun': lambda x:  sum(x) - 1})
        
        opt_res = minimize(meta_objective, x0, 
            args=(df_pd, loss_f), method='L-BFGS-B', 
            bounds=bds, constraints=cons, options={'disp': True})

        # update learner weights 
        lrnr_weights = opt_res.x.tolist()
        print(opt_res.message)
        sl_risk = meta_objective(lrnr_weights, df_pd, loss_f)

    elif meta_learner == 'auc_bfgs': # seems work, but very few updates
        # equal initial weighting
        x0 = [1/n_l]*n_l
        opt_res = minimize(meta_objective, x0, 
            args=(df_pd, loss_f), method='BFGS', options={'disp': True})
        # update learner weights
        lrnr_weights = opt_res.x.tolist()
        print(opt_res.message)
        print(lrnr_weights)
        sl_risk = meta_objective(lrnr_weights, df_pd, loss_f)

    elif meta_learner == 'auc_slsqp': # seems work, but very few iterations
        # equal initial weighting
        x0 = [1/n_l]*n_l
        bds = Bounds([0]*n_l, [1]*n_l)
        cons = ({'type': 'eq', 'fun': lambda x:  sum(x) - 1})
        opt_res = minimize(meta_objective, x0, 
            args=(df_pd, loss_f), method='SLSQP', 
            bounds=bds, constraints=cons, options={'disp': True})
        # update learner weights
        lrnr_weights = opt_res.x.tolist()
        print(opt_res.message)
        sl_risk = meta_objective(lrnr_weights, df_pd, loss_f)

    elif meta_learner == 'auc_nm': # works (bounds not work)
        # equal initial weighting
        x0 = [1/n_l]*n_l
        bds = ((0,1),) * n_l
        opt_res = minimize(meta_objective, x0, 
            args=(df_pd, loss_f), method='Nelder-Mead', 
            bounds=bds, tol=1e-6, options={'disp': True})
        # update learner weights
        lrnr_weights = opt_res.x.tolist()
        print(opt_res.message)
        # print('lalala: ', lrnr_weights)
        sl_risk = meta_objective(lrnr_weights, df_pd, loss_f)
    
    elif meta_learner == 'auc_powell': # works
        # equal initial weighting
        x0 = [1/n_l]*n_l
        # bds = Bounds([0]*n_l, [1]*n_l)
        bds = ((0,1),) * n_l
        opt_res = minimize(meta_objective, x0, 
            args=(df_pd, loss_f), method='Powell', 
            bounds=bds, tol=1e-6, options={'disp': True})
        # update learner weights
        lrnr_weights = opt_res.x.tolist()
        print(opt_res.message)
        print('lalala: ', lrnr_weights)
        sl_risk = meta_objective(lrnr_weights, df_pd, loss_f)
    
    elif meta_learner == 'auc_tnc': # not work, no update
        # equal initial weighting
        x0 = [1/n_l]*n_l
        bds = ((0,1),) * n_l
        opt_res = minimize(meta_objective, x0, 
            args=(df_pd, loss_f), method='TNC', 
            bounds=bds, tol=1e-6, options={'disp': True})
        # update learner weights
        lrnr_weights = opt_res.x.tolist()
        print(opt_res.message)
        print('lalala: ', lrnr_weights)
        sl_risk = meta_objective(lrnr_weights, df_pd, loss_f)

    # add sl info
    lrnr_names.append('lrnr_sl') 
    if normalize and min(lrnr_weights) >= 0: 
        print('normalizing weights')
        lrnr_weights = [w/sum(lrnr_weights) for w in lrnr_weights]
    lrnr_weights.append(sum(lrnr_weights)) 
    lrnr_risks.append(sl_risk) 

    # output
    df_res = pd.DataFrame({'learner': lrnr_names, 
        'weights': lrnr_weights, 'cv_risk': lrnr_risks})
    df_res_sp = spark.createDataFrame(df_res)
    return df_res_sp

# Input: initial values of learner weights
#        a dataframe with idx, outcome and cv preds of each learner
#        a loss function
def meta_objective(x, df_pd, loss_f):
    # get y
    y = df_pd['long_covid'].to_numpy()
    # get learner preds cols
    preds_cols = [c for c in df_pd if c.startswith('prob_hat_')]
    preds = df_pd[preds_cols]
    # weighted sum over all learner preds
    weighted_preds = np.sum(x * preds.to_numpy(), axis = 1)
    loss = loss_f(weighted_preds, y)
    # add penalty if weights are negative
    loss += penalty_bds(x)
    return loss

# penalize negative weights
def penalty_bds(x):
    res = int(min(x) < 0) * 10
    return res

