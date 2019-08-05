# -*- coding: utf-8 -*-
"""
Created on Tue Jul 16 12:04:29 2019

@author: horowitzb
"""

import findspark
findspark.init()

try:
    sc.stop()
except:
    pass
from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession
conf = SparkConf().setAppName("finalproject").setMaster("local[*]")
sc=SparkContext(conf = conf)
spark = SparkSession(sparkContext=sc)

import pickle
from operator import add
allData = sc.pickleFile("./all-data.pkl")

#print allData.first()

#([["Company", "Class", "Name", "City", "State", "Country", "Date"]])

#only use if there is a problem with using None
def companyOrPersonCount(x):
    if (x[0] is None):
        return (x[2], 1)
    else:
        return (x[0], 1)
        
def companyCount(x):
    if(x[0] is None):
        return ("None", 1)
    else:
        return (x[0], 1)

def companyAndPersonCount(x):
    if(x[0] is None and x[2] is None):
        return (("None", "None"), 1)
    elif(x[0] is None):
        return (("None", x[2]), 1)
    elif(x[2] is None):
        return ((x[0], "None"), 1)
    else:
        return ((x[0], x[2]), 1)
    
#sortByCompany = allData.sortBy(lambda x: [0])

#uncomment / change map function and save location based on data to parse.
allData.map(lambda x: (x[0], 1)).reduceByKey(lambda x, y: x + y).sortBy(keyfunc = lambda x: x[1], ascending = False).saveAsPickleFile("./sorted-company.pkl")
allData.map(lambda x: (x[2], 1)).reduceByKey(lambda x, y: x + y).sortBy(keyfunc = lambda x: x[1], ascending = False).saveAsPickleFile("./sorted-person.pkl")

#print tempData.take(5)

#print sortByCompany.first()
#

def countNone(item):
    nones = [0,0,0,0,0,0,0,0,1]
    for i in range(3):
        if(item[i] is None):
            nones[i] = 1
            nones[7] = 1
    if(item[3] is None):
        nones[3] = 1
        nones[4] = 1
        nones[5] = 1
        nones[7] = 1
    else:
        for i in range(3):
            if(item[3][i] is None):
                nones[3+i] = 1
                nones[7] = 1
    if(item[4] is None):
        nones[6] = 1
        nones[7] = 1
            
    return nones

#"Company", "Class", "Name", "City", "State", "Country", "Date", "Any Missing", "Total"

#print allData.first()
#nullData = allData.map(lambda x: countNone(x))
#print nullData.first()
#noneData = nullData.reduce(lambda x, y: [x[0] + y[0], x[1] + y[1], x[2] + y[2], x[3] + y[3], x[4] + y[4], x[5] + y[5], x[6] + y[6], x[7] + y[7], x[8] + y[8]])
#print noneData #[88280, 17304, 366978, 366978, 366978, 366978, 1223, 405102]
#for i in noneData:
#    print i*1.0/noneData[len(noneData)-1]
    
#0.190216395929
#0.0372848268596
#0.790725334678
#0.790725334678
#0.790725334678
#0.790725334678
#0.00263519089512
#0.872870892884
#1.0
