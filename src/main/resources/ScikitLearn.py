# Databricks notebook source
from sklearn import datasets
import pandas as pd
import numpy as np
from sklearn import tree
from sklearn.model_selection import train_test_split
from sklearn.cross_validation import train_test_split
from sklearn.tree import DecisionTreeClassifier
from sklearn.metrics import accuracy_score
from sklearn import tree

# Reading DataFrame

def findMostCommonValue(colName, df):
  return df[colName].mode()

def cleanUp(inputDf):
  for col in inputDf.columns:
    mostCommon = findMostCommonValue(col, inputDf)[0]
    inputDf[col] = inputDf[col].replace('?', mostCommon)
  # converting to numeric
  inputDf = inputDf.apply(pd.to_numeric, errors='ignore')
  return inputDf
    
def generateDecisionTree(inputDf):
  print 'Splitting data into test and train'
  features =  [col for col in inputDf.columns if col != 'Severity']
  print 'Features:%s' % features
  
  X = inputDf[features]
  y = inputDf["Severity"]
  
  X_train, X_test, y_train, y_test = train_test_split( X, y, test_size = 0.3, random_state = 100)
  clf_gini = DecisionTreeClassifier(criterion = "gini", random_state = 100,
                               max_depth=3, min_samples_leaf=5)
  clf_gini.fit(X_train, y_train)
  
  y_pred = clf_gini.predict(X_test)
  
  print "Accuracy is ", accuracy_score(y_test,y_pred)*100
  
  
df = pd.read_csv("/dbfs/FileStore/tables/mammographic_masses_data-d59c7.txt",
                 names=["BI-RADS", "Age", "Shape", "Margin", "Density", "Severity"])

cleanedUpDf = cleanUp(df)
#cleanedUpDf.info()
generateDecisionTree(cleanedUpDf)



# COMMAND ----------

# Reading datafr
