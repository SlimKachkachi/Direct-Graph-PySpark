#!/usr/bin/env python
# -*- coding: utf-8 -*-

#!/usr/bin/env python
# -*- coding: utf-8 -*-

import time
import pyspark
sc = pyspark.SparkContext()

import os

def reducer(x):
    key=x[0]
    values=x[1]
    min=key
    valuesList=[]
    listoutput=[]
    global newpaircounter
    for i in values:
        if i <min:
            min=i
        valuesList.append(i)
    if min < key:
        listoutput.append((key,min))
        for j in valuesList:
            if min != j:
              listoutput.append((j,min))
              newpaircounter.add(1)
    return listoutput


path = os.getcwd()
rdd_init = sc.textFile("/user/ccf_project/soc-pokec-relationships.txt")
rdd=rdd_init.filter(lambda x:"#" not in x).map(lambda x:x.split("\t")).map(lambda x:(int(x[0]),int(x[1])))

t0=time.time()
boucle = True


while boucle == True:
 
  newpaircounter=sc.accumulator(0)
  
  #map du ccf
  Map_rdd=rdd.flatMap(lambda x:(x,(x[1],x[0])))
  
  #fonction reduce du CCF Iterate RDD fichier de chiffres
  Reduce_rdd=Map_rdd.groupByKey().flatMap(lambda x:reducer(x))
  Reduce_rdd.count() 
  
  ##suite #fonction Reduce de CFF Dedup sur base RDD liste de chiffres pour vérifier la suppression des doublons de tuples
  rdd=Reduce_rdd.distinct().sortByKey()

  print(newpaircounter.value)
  if newpaircounter.value == 0:
    print("arret")
    boucle = False

t1=time.time()
print("c'est la fin; durée totale de :",round(t1-t0,3))