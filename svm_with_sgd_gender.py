#Linear SVM with SGD for predicting gender

from pyspark import SparkContext
# $example on$
from pyspark.mllib.classification import SVMWithSGD, SVMModel
from pyspark.mllib.regression import LabeledPoint
#import pandas as pd
# $example off$

if __name__ == "__main__":
   sc = SparkContext(appName="PythonSVMWithSGDExample")

    # $example on$
    # Load and parse the data
   # def parsePoint(line):
    #    values = [str(x) for x in line.split(',')]
     #   return LabeledPoint(values[0], values[1:])


#code, description, year, month, day, age, sex, state, police, court, source = [segs for segs in line]
'''
code = line[0]
   description = line[1]
   year = line[2]
   month = line[3]
   day = line[4]
   age = line[5]
   sex = line[6]
   state = line[7]
   police = line[8]
   court = line[9]
   source = line[10]
'''
def genderdecode(line):
   code, description, year, month, day, age, sex, state, police, court, source = [segs.strip('"') for segs in line.split(',')]
   #decoding year
   if (year == '2012'):
       y = 0
   else:
       if (year == '2013'):
           y=1
       else:
           if (year == '2014'):
               y=2
           else:
               if (year == '2015'):
                   y=3
#decoding Day of the week
   if (day == 'MONDAY'):
       d = 0
   else:
       if (day == 'TUESDAY'):
           d=1
       else:
           if (day == 'WEDNESDAY'):
               d=2
           else:
               if (day == 'THURSDAY'):
                   d=3
               else:
                   if (day == 'FRIDAY'):
                       d=4
                   else:
                       if (day == 'SATURDAY'):
                           d=5
                       else:
                           if (day == 'SUNDAY'):
                                d=6
#decoding Gender
#Since Linear SVm is binary we have only 2 values for the gender [0,1]

   if (sex == 'M'):
       s = 0
   else:
   #    if (sex == 'F'):
           s = 1
#       else:
 #          s = 2
#Decoding the Age
   if ((age >= '16') & (age <= '95')):
       age = float(age)-1
   else: age = 41.0
   if ((month >= '1') & (month <= '12')):
       month = float(month)-1
   else: month = 1.0
#Establishing the gender features as year, month and day of the week
   genderfeatures = [y, month, d]
   return LabeledPoint(s, genderfeatures)

#Load and Parse the data file based on the defined gender decode
raw_data = sc.textFile("/home/srinidhi/.linuxbrew/Cellar/spark-2.0.1-bin-hadoop2.7/Traffic_Tickets_Issued__Four_Year_Window.csv")
header = raw_data.first()
data = raw_data.filter(lambda line: line != header)
parsedData = data.map(genderdecode)
# Build the model
model = SVMWithSGD.train(parsedData, iterations=100)
# Evaluating the model on training data
labelsAndPreds = parsedData.map(lambda p: (p.label, model.predict(p.features)))
trainErr = labelsAndPreds.filter(lambda (v, p): v != p).count() / float(parsedData.count())
print("Training Error = " + str(trainErr))
# Save and load model
model.save(sc, "target3/tmp/pythonSVMWithSGDModel")
sameModel = SVMModel.load(sc, "target3/tmp/pythonSVMWithSGDModel")
# $example off$
