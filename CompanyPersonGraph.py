# -*- coding: utf-8 -*-
"""
Created on Wed Jul 17 15:33:30 2019

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

from operator import add
import matplotlib.pyplot as plt
import numpy as np
import pickle

#cOrPData = sc.pickleFile("./sorted-company-or-person.pkl")
cData = sc.pickleFile("./sorted-company.pkl")
#cAndPData = sc.pickleFile("./sorted-company-and-person.pkl")
pData = sc.pickleFile("./sorted-person.pkl")
#percentData = sc.pickleFile("./cOrP-percent.pkl")
missingData = sc.parallelize([("Company", 88280), ("Class", 17304), ("Name", 366978), ("City", 366978), ("State", 366978), ("Country", 366978), ("Date", 1223)])


#missingData = sc.parallelize(88280, 17304, 366978, 366978, 366978, 366978, 1223, 405102)
def stringInData(x, string):
    if(x is None):
        return False
    else:
        return (string.lower() in x.lower())

#how many companies have x in their name
#returns 11749    
#techInName= cAndPData.filter(lambda x: stringInData(x[0][0], "tech"))
#print techInName.count()

def tupleRddToBarChart(data, start, end):
    topNum = data.take(end)

    name = [x[0] for x in topNum][1:]
    size = [x[1] for x in topNum][1:]

    plt.figure(figsize = (20,10))
    y_pos = np.arange(len(name))
    plt.bar(y_pos, size, width = 0.5, align='center', alpha=0.5, color='b')
    plt.xticks(y_pos, name, rotation = 60)
    plt.ylabel('Number of Patents (out of 464103)')
    #plt.xlabel('Patent Holder\'s Name')
    plt.xlabel('Patent Holder\'s Company')
    #plt.title('Top 10 Individuals with the most Patents')
    plt.title('Top 10 Companies with the most Patents')

    plt.show()

tupleRddToBarChart(cData, 1, 11)


    
#returns 464103
#print cData.values().sum()
