from __future__ import unicode_literals
import sys
import operator
from array import array
import numpy as np
from numpy import *
import pandas as pd

if sys.version_info[0] < 3: 
    from StringIO import StringIO
else:
    from io import StringIO

k_input = int(sys.argv[1])

def createInputData():
    inputdata = StringIO(sys.stdin.read())
    hygdata_v3 = pd.read_csv(inputdata, sep=",")
    dataSetIN = hygdata_v3[['x','y']].values
    #dataSetIN = np.array([[1.0,1.1],[1.0,1.0],[0.0,0.0],[0.0,0.1]])
    return dataSetIN

def processData(sourceXY, dataSet, k):
    dataSetSize = dataSet.shape[0]
    #print "dataSetSize: ", dataSetSize
    diffMat = tile(sourceXY, (dataSetSize,1)) - dataSet
    #print "diffMat: ", diffMat
    sqDiffMat = diffMat**2
    sqDistances = sqDiffMat.sum(axis=1)
    distances = sqDistances**0.5
    sortedDistIndicies = distances.argsort()
    #print sortedDistIndicies
    classCount={}
    for v in range(k):
        voteIlabel = dataSet[sortedDistIndicies[v]]
        #print "==================",v
        #print voteIlabel
        classCount[v] = voteIlabel
    #
    classCountValues = classCount.values()
    #
    for v in list(classCountValues):
        print v

dataSetIN = createInputData()
processData([0.0,0.0],dataSetIN,k_input)

#gunzip -c hygdata_v3.csv.gz | python findstars.py 100

