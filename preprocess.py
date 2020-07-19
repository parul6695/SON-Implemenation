from pyspark import SparkContext, StorageLevel, SparkConf
import sys
import json
import re
import csv
import itertools
from time import time
import math
con = SparkConf().setAll([('spark.executor.memory', '8g'), ('master','local')])
sc = SparkContext(conf=con)
start = time()
ip=r"review.json"
ip2=r"business.json"

review = sc.textFile(ip).map(lambda a:(json.loads(a)['business_id'],json.loads(a)['user_id']))
business = sc.textFile(ip2).map(lambda e: (json.loads(e)['business_id'],json.loads(e)['state'])).\
    filter(lambda e:e[1]=='NV')
#.map(lambda e:(e[0][0]))
#final=business.collect()
#print(reviewRdd1.take(7))
#print(len(final))
#Taking only those reviews having business_ids in Nevada state
#f=review.filter(lambda x:x[0] in final)
join=review.join(business).map(lambda l:(l[1][0],l[0]))\
    .collect()


print("hi")
print(len(join))
#print(len(join1))
with open(r"user_business.csv",'w',newline='') as n:
    file_object=csv.writer(n)
    file_object.writerow(['user_id','business_id'])
    file_object.writerows(join)



#print(reviewRdd)newline=''


