# -*- coding: utf-8 -*-
"""
Created on Mon Jul 15 21:08:05 2019

@author: horowitzb
"""


import findspark
findspark.init()

# create entry points to spark
try:
    sc.stop()
except:
    pass
from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession
conf = SparkConf().setAppName("finalproject").setMaster("local[*]")
sc=SparkContext(conf = conf)
spark = SparkSession(sparkContext=sc)

import re

def toCSVLine(data):
    return '\t'.join(str(d) for d in data)

import pickle

testSum = 0
allData = sc.emptyRDD()

for y in range (1900, 2020):
    try:
        with open('./parsed-data/{}-parsed.pkl'.format(y), 'rb') as f:
            data = pickle.load(f)
            year = sc.parallelize(data)
            year.map(lambda x: (x[0], 1)).reduceByKey(lambda x, y: x + y).sortBy(keyfunc = lambda x: x[1], ascending = False).saveAsPickleFile("./company-by-year/{}.pkl".format(y))
            year.map(lambda x: (x[2], 1)).reduceByKey(lambda x, y: x + y).sortBy(keyfunc = lambda x: x[1], ascending = False).saveAsPickleFile("./person-by-year/{}.pkl".format(y))
            #testSum += year.count()
            allData = allData.union(year)
    except (EOFError):
        continue
        #print("year {} cannot be depickled".format(y))


#print allData.take(15)
#print allData.filter(lambda x: x[0] == "PHILIP MORRIS PRODUCTS S.A.").collect()
#print testSum
#print allData.count()
#print allData.first()
allData.saveAsPickleFile("./all-data.pkl")
#allData = allData.map(lambda x: toCSVLine(x))
#df = allData.toDF()
#df.write.csv(path= "./sample_file.csv", mode="append", header="true")