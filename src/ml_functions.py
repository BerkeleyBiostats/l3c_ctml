
# pip install pyspark
# pip install shap

#--------------------#
#    Import Modules
#--------------------#
import pyspark
from pyspark.sql import SparkSession
import pandas as pd
import numpy as np
import shap
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

# Init Spark Session
spark = SparkSession.builder.getOrCreate()


#--------------------#
#     SL Config
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

def train_sl(analytic_full_train):
    analytic_full = analytic_full_train
    # base fit
    train_res = cv_train(analytic_full)
    # meta fit
    meta_res = meta_train(train_res)
    return meta_res  

def targeted_ml_team_predictions(train_sl, analytic_full_train, analytic_final_test):
    # get lrnr weights
    df_metalearner =  train_sl.toPandas()
    wts = df_metalearner['weights'].to_numpy()[:-1]
    lrnr_names = df_metalearner['learner'].to_numpy()[:-1]
    # fit each learner on full data, get predictions on test data
    df_t = analytic_full_train
    df_t_columns = list(analytic_full_train.columns)
    df_v = analytic_final_test
    df_v_columns = set(list(analytic_final_test.columns))

    for col in df_t_columns:
        if col not in df_v_columns:
            df_v = df_v.withColumn(col, F.lit(0))

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

#--------------------#
#     Shapley Plots
#--------------------#
def plots_fotmatted(analytic_full_train, Metatable, lrnr_one = lrnr_gb200, n_screen = 0):
    # data
    df = analytic_full_train
    # fit model on full data 
    print("fit model on full data")
    model_fit, X, selected_covariates = fit_model_vim(df, lrnr_one, n_screen)
    # get shap values
    print("get shap values")
    explainer = shap.Explainer(model_fit)
    X_pd = X.toPandas()
    shap_values = explainer(X_pd, check_additivity=False)
    n_covars = len(selected_covariates)

    # get shap plot on each feature category level
    print("get shap plot on each feature category level")
    df_meta = Metatable.filter(~Metatable.Name.isin(['long_covid', 'person_id']))
    bad_cols_meta = ['Name', 'Type', 'Formatted', 'Category']
    cate_cols = [c for c in df_meta.columns if c not in bad_cols_meta]
    print(cate_cols)
    for cate_col in cate_cols:
        df_meta_pd = df_meta.select("Name", cate_col).toPandas()
        # temp
        if cate_col == 'Pathways':
            df_meta_pd["Pathways"] = np.where(df_meta_pd["Pathways"] == '', 
                'demographics_anthro' , df_meta_pd["Pathways"])
        elif cate_col == 'Temporal':
            df_meta_pd["Temporal"] = np.where(df_meta_pd["Temporal"] == 'covid', 
                'Acute COVID' , df_meta_pd["Temporal"])
            df_meta_pd["Temporal"] = np.where(df_meta_pd["Temporal"] == 'pre covid', 
                'Pre-COVID' , df_meta_pd["Temporal"])
            df_meta_pd["Temporal"] = np.where(df_meta_pd["Temporal"] == 'post covid', 
                'Post-COVID' , df_meta_pd["Temporal"])
            df_meta_pd["Temporal"] = np.where(df_meta_pd["Temporal"] == 'pre pre covid', 
                'Baseline' , df_meta_pd["Temporal"])
        plot_shap_group(shap_values, cate_col, df_meta_pd, selected_covariates)
    
    # get shap plots on feature level
    print("get shap plots on feature level")
    df_meta_pd = df_meta.toPandas()
    plot_shap(shap_values, df_meta_pd, "Individual_Features (p = %0.0f)" % n_covars)
        
def plots_simple(analytic_full_train, lrnr_one = lrnr_gb20, n_screen = 0):
    # data
    df = analytic_full_train
    # fit model on full data 
    print("fit model on full data")
    model_fit, X, selected_covariates = fit_model_vim(df, lrnr_one, n_screen)
    # get shap values
    print("get shap values")
    explainer = shap.Explainer(model_fit)
    X_pd = X.toPandas()
    shap_values = explainer(X_pd, check_additivity=False)
    n_covars = len(selected_covariates)
    
    # get shap plots on feature level
    # print("get shap plots on feature level")
    # plot_shap(shap_values, df_meta_pd, "Individual_Features (p = %0.0f)" % n_covars)
    var_cate = "Individual_Features (p = %0.0f)" % n_covars

    # plot shapley bar
    fig, ax = plt.subplots()
    shap.plots.bar(shap_values, max_display=20, show = False)
    plt.gcf().set_size_inches(15,10)

    plt.yticks(fontsize=7, rotation=45)
    plt.title("Covariate Pool: " + var_cate)
    txt = 'Figure 1. Bar plot of most important model features associated with PASC. \n For additional information regarding covariates, see metatable.'
    fig.text(.5, .01, txt, ha='center', fontsize = 15)
    plt.show()

    # plot shapley beeswarm
    fig, ax = plt.subplots()
    shap.plots.beeswarm(shap_values, max_display=20, show = False)
    plt.gcf().set_size_inches(16,10)
    plt.yticks(fontsize=7, rotation=45)
    plt.title("Covariate Pool: " + var_cate)
    txt = 'Figure 2. Beeswarm plot of most important model features associated with PASC. \n For additional information regarding covariates, see metatable.'
    fig.text(.5, .01, txt, ha='center', fontsize = 15)
    plt.show()
    
def fit_model_vim(df, lrnr_one, n_screen, covariates = None):
    # for simplicity, first 9000 
    # df = df.limit(9000)
    bad_cols = ['long_covid', 'person_id', 'fold_id', 'idx', 'in_train']
    if covariates is None:
        covariates = [col for col in df.columns if col not in bad_cols]
    
    # assemble features
    assembler = VectorAssembler(
        inputCols = covariates,
        outputCol = "features"
        )
    # fit on training set
    model_fit = lrnr_one.fit(assembler.transform(df))

    return model_fit, df.select(*covariates), covariates


def plot_shap(shap_values, df_meta, var_cate = "all"):
    # plot shapley bar
    fig, ax = plt.subplots()
    shap.plots.bar(shap_values, max_display=20, show = False)
    plt.gcf().set_size_inches(15,10)
    # (begin) format feature labels
    varnames = [item.get_text() for item in ax.get_yticklabels()]
    df_varnames = pd.DataFrame({'Name': varnames})
    df_metalabels = df_meta[['Name', 'Formatted']]
    df_plotlabels = df_varnames.merge(df_metalabels, on = 'Name', how='left')
    df_plotlabels = df_plotlabels.fillna('')
    df_plotlabels["Formatted"] = np.where(df_plotlabels["Formatted"] == '', 
                                        df_plotlabels["Name"] , 
                                        df_plotlabels["Formatted"])
    labels = df_plotlabels["Formatted"].to_numpy()
    ax.set_yticklabels(labels)
    # (end) format feature labels
    plt.yticks(fontsize=7, rotation=45)
    plt.title("Covariate Pool: " + var_cate)
    txt = 'Figure 1. Bar plot of most important model features associated with PASC. \n For additional information regarding covariates, see metatable.'
    fig.text(.5, .01, txt, ha='center', fontsize = 15)
    plt.show()

    # plot shapley beeswarm
    fig, ax = plt.subplots()
    shap.plots.beeswarm(shap_values, max_display=20, show = False)
    plt.gcf().set_size_inches(16,10)
    # (begin) format feature labels
    ax.set_yticklabels(np.flip(labels))
    # (end) format feature labels
    plt.yticks(fontsize=7, rotation=45)
    plt.title("Covariate Pool: " + var_cate)
    txt = 'Figure 2. Beeswarm plot of most important model features associated with PASC. \n For additional information regarding covariates, see metatable.'
    fig.text(.5, .01, txt, ha='center', fontsize = 15)
    plt.show()


def plot_shap_group(shap_values, cate_col, df_meta, covariates, top_n = 10):
    # calc group level shap values
    shap_values_copy = np.copy(shap_values.values)
    importance = np.mean(np.abs(shap_values_copy), axis = 0)
    df_imp = pd.DataFrame({'Importance': importance, 'Name': covariates})
    # df = df_imp.merge(df_meta, on='Name', how='left').groupby(cate_col).agg('mean').reset_index()
    df = df_imp.merge(df_meta, on='Name', how='left')
    df = df.sort_values([cate_col,'Importance'],ascending=False).groupby(cate_col).head(top_n)
    df = df.groupby(cate_col).agg('mean').reset_index()
    df = df.sort_values("Importance", ascending=False).head(20)
    df.index = df[cate_col]
    n_cate = df.shape[0]

    # plot shapley bar on group level
    fig, ax = plt.subplots(figsize=(12, 10))
    sns.barplot(x=df["Importance"], y=df[cate_col], palette=sns.color_palette("RdYlBu", df.shape[0]))
    plt.yticks(rotation=45)
    plt.title("Covariate Pool: " + cate_col + "_Features (p = %0.0f)" % n_cate)
    ax.set(ylabel='')
    # temp
    if cate_col == 'Temporal':
        txt = 'Figure 3. Variable importance by the temporal window. Ranked by the mean absolute Shapley value \n of the top 10 features in each category. Baseline (prior to t - 37); pre-COVID (t - 37 to t - 7), \n acute COVID (t - 7 to t + 14), and post-COVID (t + 14 to t + 28), with t being the index COVID date.'
        fig.text(.5, 0, txt, ha='center', fontsize = 13)
    elif cate_col == 'Pathways':
        txt = 'Figure 4. Variable importance by biological pathway. Ranked by the mean absolute Shapley value \n of the top 10 features (ranked by the same metric) in each category. \n For additional information regarding covariates, see metatable.'
        fig.text(.5, 0, txt, ha='center', fontsize = 13)
    plt.show()
    return None



