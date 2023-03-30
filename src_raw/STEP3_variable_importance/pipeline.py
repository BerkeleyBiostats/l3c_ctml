import pandas as pd
import numpy as np
import shap
import matplotlib.pyplot as plt
import seaborn as sns
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
spark = SparkSession.builder.getOrCreate()

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
# from xgboost.spark import SparkXGBClassifier as Lrnr_xgb

from pyspark.ml.evaluation import BinaryClassificationEvaluator
from pyspark.ml.tuning import ParamGridBuilder, CrossValidator

# from sl3py.learners import *

# define lr lib
lrnr_gb20 = Lrnr_gb(labelCol='long_covid', maxIter = 20)
lrnr_gb150 = Lrnr_gb(labelCol='long_covid', maxIter = 150)
lrnr_gb180 = Lrnr_gb(labelCol='long_covid', maxIter = 180)
lrnr_gb190 = Lrnr_gb(labelCol='long_covid', maxIter = 190)
lrnr_gb200 = Lrnr_gb(labelCol='long_covid', maxIter = 200) # (auc ~0.90)
lrnr_gb220 = Lrnr_gb(labelCol='long_covid', maxIter = 220)
lrnr_gb250 = Lrnr_gb(labelCol='long_covid', maxIter = 250)
lrnr_gb275 = Lrnr_gb(labelCol='long_covid', maxIter = 275)
lrnr_gb300 = Lrnr_gb(labelCol='long_covid', maxIter = 300)

@transform_pandas(
    Output(rid="ri.vector.main.execute.5048ca2d-26fc-4fca-a9e4-61e235891838"),
    Metatable=Input(rid="ri.foundry.main.dataset.6f1d6d9a-9bc8-437c-8a4b-ce02d6ec5ef4"),
    analytic_full_train=Input(rid="ri.vector.main.execute.d0868612-0378-4999-9dd9-1da6ad4f010c")
)
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
    

@transform_pandas(
    Output(rid="ri.vector.main.execute.ecd5da5f-b269-4b8b-b537-d042ed54c64d"),
    Metatable=Input(rid="ri.foundry.main.dataset.6f1d6d9a-9bc8-437c-8a4b-ce02d6ec5ef4"),
    analytic_full_train=Input(rid="ri.vector.main.execute.d0868612-0378-4999-9dd9-1da6ad4f010c")
)
def vim_shap(analytic_full_train, Metatable, lrnr_one = lrnr_gb200, n_screen = 0):
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
    bad_cols_meta = ['Name', 'Type', 'Formatted']
    cate_cols = [c for c in df_meta.columns if c not in bad_cols_meta]
    print(cate_cols)
    for cate_col in cate_cols:
        df_meta_pd = df_meta.select("Name", cate_col).toPandas()
        # temp
        if cate_col == 'Pathways':
            df_meta_pd["Pathways"] = np.where(df_meta_pd["Pathways"] == '', 
                'demographics_anthro' , df_meta_pd["Pathways"])
        plot_shap_group(shap_values, cate_col, df_meta_pd, selected_covariates)
    
    # get shap plots on feature level
    print("get shap plots on feature level")
    df_meta_pd = df_meta.toPandas()
    plot_shap(shap_values, df_meta_pd, "Individual_Features (p = %0.0f)" % n_covars)
        

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
    plt.show()

    # plot shapley beeswarm
    fig, ax = plt.subplots()
    shap.plots.beeswarm(shap_values, max_display=20, show = False)
    plt.gcf().set_size_inches(15,10)
    # (begin) format feature labels
    ax.set_yticklabels(np.flip(labels))
    # (end) format feature labels
    plt.yticks(fontsize=7, rotation=45)
    plt.title("Covariate Pool: " + var_cate)
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
    # plt.figure(figsize=(10, 8))
    fig, ax = plt.subplots(figsize=(10, 8))
    sns.barplot(x=df["Importance"], y=df[cate_col], palette=sns.color_palette("RdYlBu", df.shape[0]))
    plt.yticks(rotation=45)
    plt.title("Covariate Pool: " + cate_col + "_Features (p = %0.0f)" % n_cate)
    ax.set(ylabel='')
    plt.show()
    return None
    

