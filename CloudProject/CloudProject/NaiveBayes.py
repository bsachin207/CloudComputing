import sys
import numpy as np
import datetime
from pyspark import SparkContext
from datetime import date
from pyspark.sql import SQLContext
from pyspark.sql.types import *
from calendar import week
from chardet.test import count


if __name__ == "__main__":
    if len(sys.argv) !=2:
        print >> sys.stderr, "Usage: linreg <datafile>"
        exit(-1)
sc = SparkContext(appName="naivebayes")
sqlContext=SQLContext(sc)
inputCrimeCSV = sc.textFile(sys.argv[1])
header = inputCrimeCSV.first()
inputCrimeCSV = inputCrimeCSV.filter(lambda x:x !=header)

schemaString="day timeslot block crimetype locationdescription latitude longitude"


crimeData = inputCrimeCSV.map(lambda line: (line.split(',')))#spliting only y part

def date2dayofweek(f):
    f=f.split('/')
    g=f[0]+" "+f[1]+" "+f[2]
    day=datetime.datetime.strptime(g,'%m %d %Y').strftime('%A')
    return day
#datetime.datetime.strptime(line[2].split(' ',1)[0].split('/'),'%m,%d,%y').strftime('%A')
def timeslot(f):
    time24=datetime.datetime.strptime(f,'%I:%M:%S %p').strftime('%X')
    timesl=int(time24[0:2])/3
    timesl=timesl+1#divided time into 8 slots 
    return timesl
    
fields = [StructField(field_name, StringType(), True) for field_name in schemaString.split()]
schema = StructType(fields)

reformattedCrime=crimeData.map(lambda line: [date2dayofweek(line[2].split(' ',1)[0]),timeslot(line[2].split(' ',1)[1]),line[3].split(' ',1)[1],line[5],line[7],line[19],line[20]])

#train_set, test_set = reformattedCrime.randomSplit([0.0001, 0.9999])

train_set = reformattedCrime.take(500)

schemaCrime = sqlContext.createDataFrame(train_set, schema)
schemaCrime.registerTempTable("chicagocrimedata")

locationVocabulary = sqlContext.sql("SELECT count(distinct(block)) from chicagocrimedata").collect()[0][0]
timeVocabulary = sqlContext.sql("SELECT count(distinct(timeslot)) from chicagocrimedata").collect()[0][0]
dayVocabulary = sqlContext.sql("SELECT count(distinct(day)) from chicagocrimedata").collect()[0][0]


locationsMatrix=sqlContext.sql("SELECT crimetype,block,count(*) AS countPerBlock FROM chicagocrimedata group by crimetype,block order by countPerBlock desc")
timeMatrix=sqlContext.sql("SELECT crimetype,timeslot,count(*) AS countPerTime FROM chicagocrimedata group by crimetype,timeslot order by countPerTime desc")
dayMatrix=sqlContext.sql("SELECT crimetype,day,count(*) AS countPerDay FROM chicagocrimedata group by crimetype,day order by countPerDay desc")




#Extract all classes. Here, distinct crime types 
CrimeTypes = sqlContext.sql("SELECT distinct(crimetype) AS crimetypes FROM chicagocrimedata").collect()

allCrimeTypes = list()
for index in range(len(CrimeTypes)):
    allCrimeTypes.append(CrimeTypes[index][0])
    
#Extracting statistics of crimes  
crimeCounts=sqlContext.sql("SELECT crimetype,count(*) as crimeCount FROM chicagocrimedata GROUP BY crimetype").collect()
countByCrimeType = {}
for index in range(len(crimeCounts)):
    countByCrimeType[crimeCounts[index].crimetype] = crimeCounts[index].crimeCount

print countByCrimeType.items()
           
'''
#Registering DataFrames as a table for program efficiency.
locationsMatrix.registerTempTable("LocationMatrix")
timeMatrix.registerTempTable("TimeMatrix")
dayMatrix.registerTempTable("DayMatrix")
#crimeCounts.registerTempTable("CrimeCounts")






#sqlContext.sql("UPDATE TABLE LocationMatrix SET block = 'N STATE ST' WHERE crimetype='THEFT and count=4")
#testing = sqlContext.sql("SELECT block, count(distinct(crimetype)) AS count FROM LocationMatrix group by crimetype,block order by count desc")

#print testing.show(100)

userlocation = "N STATE ST"
usertimeslot = intern('3')
userday = "Friday"

Loc_nOfCrime = {}
time_nOfCrime = {}
day_nOfCrime = {}



for index in range(len(allCrimeTypes)):
    temp = sqlContext.sql("SELECT countPerBlock FROM LocationMatrix WHERE block='"+ userlocation+ "' and crimetype='"+allCrimeTypes[index]+"'").collect()
    if (not temp):
        Loc_nOfCrime[allCrimeTypes[index]] = 0
    else:
        Loc_nOfCrime[allCrimeTypes[index]] = temp[0][0]
 
#Loc_nOfCrime[allCrimeTypes[index]] = temp[0].Pcount

for index in range(len(allCrimeTypes)):
    temp = sqlContext.sql("SELECT countPerTime FROM TimeMatrix WHERE timeslot="+ usertimeslot+ " and crimetype='"+allCrimeTypes[index]+"'").collect()
    if (not temp):
        time_nOfCrime[allCrimeTypes[index]] = 0
    else:
        time_nOfCrime[allCrimeTypes[index]] = temp[0][0]
        
        
for index in range(len(allCrimeTypes)):
    temp = sqlContext.sql("SELECT countPerDay FROM DayMatrix WHERE day='"+ userday+ "' and crimetype='"+allCrimeTypes[index]+"'").collect()
    if (not temp):
        day_nOfCrime[allCrimeTypes[index]] = 0
    else:
        day_nOfCrime[allCrimeTypes[index]] = temp[0][0]
        

print Loc_nOfCrime.items()
print time_nOfCrime.items()
print day_nOfCrime.items()


#print testing.show(100)
#print timeMatrix.filter(timeMatrix.crimetype == 'THEFT' & timeMatrix.timeslot==3).show(100)




#print locationVocabulary
#print locationsMatrix.show(100)

'''

sc.stop()    