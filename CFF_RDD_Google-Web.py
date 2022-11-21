{\rtf1\ansi\ansicpg1252\cocoartf2639
\cocoatextscaling0\cocoaplatform0{\fonttbl\f0\fnil\fcharset0 Menlo-Regular;}
{\colortbl;\red255\green255\blue255;\red0\green0\blue0;}
{\*\expandedcolortbl;;\csgray\c0;}
\paperw11900\paperh16840\margl1440\margr1440\vieww11520\viewh11620\viewkind0
\pard\tx560\tx1120\tx1680\tx2240\tx2800\tx3360\tx3920\tx4480\tx5040\tx5600\tx6160\tx6720\pardirnatural\partightenfactor0

\f0\fs22 \cf2 \CocoaLigature0 #!/usr/bin/env python\
# -*- coding: utf-8 -*--\
\
import time\
import pyspark\
sc = pyspark.SparkContext()\
sc.setLogLevel("ERROR")\
\
import os\
\
def reducerYield2(x):\
    key=x[0]\
    values=x[1]\
    min=key\
    valuesList=[]\
    #listoutput=[]\
    for i in values:\
        if i <min:\
            min=i\
        valuesList.append(i)\
    if min < key:\
        yield((key,min))\
        for j in valuesList:\
            if min != j:\
                newpaircounter.add(1)\
                yield((j,min))\
\
\
\
rdd_prep = sc.textFile("/students/iasd_20222023/skachkachi/datasets/facebook_combined.txt")\
print('Number of partitions before: \{\}'.format(rdd_prep.getNumPartitions()))\
\
rdd_init=rdd_prep.repartition(4)\
print('Number of partitions after: \{\}'.format(rdd_init.getNumPartitions()))\
\
rdd=rdd_init.map(lambda x:(x.split(' ')))\
\
\
boucle = True\
t0=time.time()\
iterationId=int(0)\
\
while boucle == True:\
  iterationId +=1\
  newpaircounter=sc.accumulator(0)\
\
  #map du ccf\
  rdd=rdd.flatMap(lambda x:(x,(x[1],x[0])))\
\
  #fonction reduce du CCF Iterate RDD fichier de chiffres\
  rdd=rdd.groupByKey().flatMap(lambda x:reducerYield2(x)) #.filter(lambda x:x!=None).flatMap(lambda x:x)\
  #Reduce_rdd.count()\
\
  ##suite #fonction Reduce de CFF Dedup sur base RDD liste de chiffres pour v\'e9rifier la suppression des doublons de tuples\
  rdd=rdd.distinct().sortByKey()\
\
  print("iteration n\'b0 :",iterationId, "nombre de paires cr\'e9\'e9es : ", newpaircounter.value)\
  if newpaircounter.value == 0:\
    print("arret")\
    boucle = False\
\
t1=time.time()\
print("c'est la fin; dur\'e9e totale de :",round(t1-t0,3))\
}