#!/usr/bin/env python
# -*- coding: utf-8 -*-

import time
import pyspark

rom pyspark.sql import SparkSession 
spark = SparkSession.builder \
  .appName('Slim Essai Spark DataFrames') \
  .getOrCreate()

from pyspark.sql.functions import *
from pyspark.sql.types import ArrayType,IntegerType,MapType


@udf(ArrayType(ArrayType(IntegerType())))
def DFreducerBis(key,value):
    min=key
    valuesList=[]
    listoutput=[]
#   global accu
    for i in value:
        if i <min: 
            min=i
        valuesList.append(i)
    if min < key:
        listoutput.append((0,key,min))
        for j in valuesList:
            if min != j:
              listoutput.append((1,j,min))
#             accu.add(1)
#   print(listoutput)
    return listoutput

DF_Init=spark.read.csv("/user/ccf_project/soc-pokec-relationships.txt",sep="\t")

DF=DF_Init.withColumn("_c0",DF_Init["_c0"].cast("int")).withColumnRenamed("_c0","key")\
                     .withColumn("_c1",DF_Init["_c1"].cast("int")).withColumnRenamed("_c1","value")

#Initialisation de la boucle
boucle = True
t0=time.time()

while boucle == True:

  #map du CFF
  MapDF=DF.union(DF.select('value','key')).groupBy("key").agg(collect_list("value").alias("MapOutput"))

  #fonction reduce du CCF Iterate DF fichier de chiffres
  ReduceDF=MapDF.withColumn("ListnewPairs",DFreducerBis("key","MapOutput")).select(explode(col('ListnewPairs'))).select([col("col")[i] for i in range(3)])

  compteur=ReduceDF.where("col[0]=1").count()
  print(compteur)

  ##suite #fonction Reduce de CFF Dedup sur base RDD liste de chiffres pour vérifier la suppression des doublons de tuples
  DF=ReduceDF.withColumnRenamed("col[1]","key").withColumnRenamed("col[2]","value").select("key","value").distinct()

  if compteur == 0:
    print("arret")
    boucle = False

t1=time.time()
#le round de python interfère l'opérateur round de pspark sql (qui s'applique à des colonnes)
print("c'est la fin; durée totale de :",t1-t0)