import sys
import numpy as np
import datetime
from pyspark import SparkContext
from datetime import date
from pyspark.sql import SQLContext
from pyspark.sql.types import *
from compiler.syntax import check
from numpy.oldnumeric.random_array import seed
import operator
import math
import pylab as plt



if __name__ == "__main__":
    if len(sys.argv) !=3:
        print len(sys.argv)
        print >> sys.stderr, "Usage: NaiveBayes.py <datafile> <'location,timeslot,day'>"
        exit(-1)
               
def barplot(d):
    plt.bar(range(1,len(d)+1), list(d.values()), align='center')
    plt.xticks(range(1,len(d)+1),list(d))
    plt.show()     

#setting up spark Context.
sc = SparkContext(appName="NaiveBayes")

# ALPHA is a smoothing factor.
S_ALPHA = 1
sqlContext=SQLContext(sc)

#Reading the Input CSV file and user inputs.
inputCrimeCSV = sc.textFile(sys.argv[1])
userInput = sys.argv[2].split(',')

userlocation = userInput[0]
usertimeslot = userInput[1]
userday = userInput[2]

header = inputCrimeCSV.first()
inputCrimeCSV = inputCrimeCSV.filter(lambda x:x !=header)

#Splitting the input file
inputCSV,excludecsv=inputCrimeCSV.randomSplit([0.999,0.001])

#inputCSV = inputCrimeCSV.take(500)


crimeData = inputCSV.map(lambda line: (line.split(',')))
crimeData = (sc.parallelize(inputCSV)).map(lambda line: (line.split(',')))

# Finding out the Day of the week From the Date
def date2dayofweek(f):
    f=f.split('/')
    g=f[0]+" "+f[1]+" "+f[2]
    day=datetime.datetime.strptime(g,'%m %d %Y').strftime('%A')
    return day

#Creating 8 time slots in a day. 3 per hour
def timeslot(f):
    time24=datetime.datetime.strptime(f,'%I:%M:%S %p').strftime('%X')
    timesl=int(time24[0:2])/3
    timesl=timesl+1#divided time into 8 slots 
    return timesl

#Schema for creating the table.  
schemaString="day timeslot block crimetype latitude longitude" 
#fields = [StructField(field_name, StringType(), True) for field_name in schemaString.split()]
#schema = StructType(fields)


reformattedCrime=crimeData.map(lambda line: [date2dayofweek(line[1].split(' ',1)[0]),timeslot(line[1].split(' ',1)[1]),line[2].split(' ',1)[1],line[3],line[4],line[5]])

#Creating table from the input file.
schemaCrime = sqlContext.createDataFrame(reformattedCrime, ['day','timeslot','block','crimetype','latitude','longitude'])
schemaCrime.registerTempTable("chicagocrimedata")
sqlContext.cacheTable("chicagocrimedata")

#Caching helps in the retrieving the data faster
locationVocabulary = sqlContext.sql("SELECT count(distinct(block)) from chicagocrimedata").collect()[0][0]
timeVocabulary = sqlContext.sql("SELECT count(distinct(timeslot)) from chicagocrimedata").collect()[0][0]
dayVocabulary = sqlContext.sql("SELECT count(distinct(day)) from chicagocrimedata").collect()[0][0]
TOTALCRIMES = sqlContext.sql("SELECT count(distinct(day)) from chicagocrimedata").collect()[0][0]

#Creating matrix for NaiveBayes calculations.
locationsMatrix=sqlContext.sql("SELECT crimetype,block,count(*) AS countPerBlock FROM chicagocrimedata group by crimetype,block order by countPerBlock desc")
timeMatrix=sqlContext.sql("SELECT crimetype,timeslot,count(*) AS countPerTime FROM chicagocrimedata group by crimetype,timeslot order by crimetype")
dayMatrix=sqlContext.sql("SELECT crimetype,day,count(*) AS countPerDay FROM chicagocrimedata group by crimetype,day order by countPerDay desc")

#Extract all classes. Here, distinct crime types 
CrimeTypes = sqlContext.sql("SELECT distinct(crimetype) AS crimetypes FROM chicagocrimedata order by crimetypes").collect()
allCrimeTypes = list()
for index in range(len(CrimeTypes)):
    allCrimeTypes.append(CrimeTypes[index][0])
    
#Extracting statistics of crimes   in the countByCrimeType dictionary
crimeCounts=sqlContext.sql("SELECT crimetype,count(*) as crimeCount FROM chicagocrimedata GROUP BY crimetype order by crimeCount").collect()
countByCrimeType = {}
for index in range(len(crimeCounts)):
    countByCrimeType[crimeCounts[index].crimetype] = crimeCounts[index].crimeCount

#Un-caching the table
sqlContext.uncacheTable("chicagocrimedata")         

#Registering DataFrames as a table for program efficiency.
locationsMatrix.registerTempTable("LocationMatrix")
timeMatrix.registerTempTable("TimeMatrix")
dayMatrix.registerTempTable("DayMatrix")
#sqlContext.cacheTable("TimeMatrix")

'''
userlocation = "S WABASH AVE"
usertimeslot = 1     #For Battery
userday = "Wednesday"
'''


temp_Loc = sqlContext.sql("SELECT crimetype, countPerBlock FROM LocationMatrix WHERE block='"+ userlocation+ "' order by crimetype").collect()
temp_time = sqlContext.sql("SELECT crimetype, countPerTime FROM TimeMatrix WHERE timeslot="+str(usertimeslot)+" order by crimetype").collect()
temp_day =sqlContext.sql("SELECT crimetype, countPerDay FROM DayMatrix WHERE day='"+ userday+ "' order by crimetype").collect()

loc_nOfCrime = dict.fromkeys(allCrimeTypes,S_ALPHA)
time_nOfCrime = dict.fromkeys(allCrimeTypes,S_ALPHA)
day_nOfCrime = dict.fromkeys(allCrimeTypes,S_ALPHA)

#Pre-Calculation for Posterior Probability
for index in range(len(temp_Loc)):
    loc_nOfCrime[temp_Loc[index].crimetype] = loc_nOfCrime[temp_Loc[index].crimetype] + temp_Loc[index].countPerBlock
for index in range(len(temp_time)):    
    time_nOfCrime[temp_time[index].crimetype] = time_nOfCrime[temp_time[index].crimetype]+temp_time[index].countPerTime
for index in range(len(temp_day)):
    day_nOfCrime[temp_day[index].crimetype] = day_nOfCrime[temp_day[index].crimetype]+temp_day[index].countPerDay

#Creating Dictionary for Probabilities of all crime types.
probabilities = dict.fromkeys(allCrimeTypes,1)
for crime in loc_nOfCrime:
    locationPrbability = math.log((loc_nOfCrime[crime]*(countByCrimeType[crime]/float(TOTALCRIMES)))/float(locationVocabulary+S_ALPHA*countByCrimeType[crime]))
    timeProbability = math.log(time_nOfCrime[crime]/float(timeVocabulary+S_ALPHA*countByCrimeType[crime]))
    dayProbability = math.log(day_nOfCrime[crime]/float(dayVocabulary+S_ALPHA*countByCrimeType[crime]))
    probabilities[crime] = locationPrbability + timeProbability + dayProbability+10

#Top 3 Crimes with highest probabilities are shown.    
sorted_x = dict(sorted(probabilities.items(), key=operator.itemgetter(1),reverse=True)[:3])

#Showing the result to the user.
print "You may encounter below 3 crimes: "
print list(sorted_x)[0]
print list(sorted_x)[1]
print list(sorted_x)[2]


'''
barplot(sorted_x)
plt.pie(countByCrimeType.values(),  labels=list(countByCrimeType),  autopct='%1.1f%%', shadow=True, startangle=140)
plt.axis('equal')
plt.show()
'''

sc.stop()    