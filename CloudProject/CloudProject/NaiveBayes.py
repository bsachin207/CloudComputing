import sys
import numpy as np
import datetime
from pyspark import SparkContext
from datetime import date
from pyspark.sql import SQLContext
from pyspark.sql.types import *
from calendar import week


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




train_set = reformattedCrime.take(200)

schemaCrime = sqlContext.createDataFrame(train_set, schema)
schemaCrime.registerTempTable("chicagocrimedata")
Weekday1 = 'day'
results=sqlContext.sql("SELECT * FROM chicagocrimedata order by block")

print results.show(200)

#print"reformatted crime",results.take(20)


sc.stop()    