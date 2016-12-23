# Binary Decision Tree algorithm for predicting the Age given the Gender, Year, Month & Day of Week
from pyspark.mllib.regression import LabeledPoint
from pyspark.mllib.tree import DecisionTree
# Create rdd of dataset
raw2_rdd = sc.textFile("/Users/GSCHARITOS/Project/Traffic_Tickets_Issued__Four_Year_Window.csv")
raw2_rdd.count()
raw2_rdd.take(5)
# Remove header
header = raw2_rdd.first()
dataset_rdd = raw2_rdd.filter(lambda line: line != header)
dataset_rdd.takeSample(False, 5, 0)
# Decoder function to return labeled point for each row
def ageencode(line):
    code, description, year, month, day, age, sex, state, police, court, source = [segs.strip('"') for segs in line.split(',')]
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
    if (sex == 'M'):
        s = 0
    else:
        if (sex == 'F'):
            s = 1
        else:
            s = 2
    if ((age >= '16') & (age <= '20')):
        age = 0
    else :
        if ((age >= '21') & (age <= '25')):
            age = 1
        else :
            if ((age >= '26') & (age <= '30')):
                age = 2
            else :
                if ((age >= '31') & (age <= '35')):
                    age = 3
                else :
                    if ((age >= '36') & (age <= '40')):
                        age = 4
                    else :
                        if ((age >= '41') & (age <= '45')):
                            age = 5
                        else :
                            if ((age >= '46') & (age <= '50')):
                                age = 6
                            else :
                                if ((age >= '51') & (age <= '55')):
                                    age = 7
                                else :
                                    if ((age >= '56') & (age <= '60')):
                                        age = 8
                                    else :
                                        if ((age >= '61') & (age <= '65')):
                                            age = 9
                                        else :
                                            if ((age >= '66') & (age <= '70')):
                                                age = 10
                                            else :
                                                if ((age >= '71') & (age <= '75')):
                                                    age = 11
                                                else :
                                                    if ((age >= '76') & (age <= '80')):
                                                        age = 12
                                                    else :
                                                        if ((age >= '81') & (age <= '85')):
                                                            age = 13
                                                        else :
                                                            if ((age >= '86') & (age <= '90')):
                                                                age = 14
                                                            else :
                                                                if ((age >= '91') & (age <= '96')):
                                                                    age = 15
                                                                else : age = 5
    if ((month >= '1') & (month <= '12')):
        month = float(month)-1
    else: month = 1.0
    agefeatures = [y, month, d, s]
    return LabeledPoint(age, agefeatures)

# Create labeled points rdd
agelabeled_points_rdd = dataset_rdd.map(ageencode)
agelabeled_points_rdd.takeSample(False, 5, 0)
# Create training & test set rdd
agetraining_rdd, agetest_rdd = agelabeled_points_rdd.randomSplit([0.9, 0.1], seed = 0)
agetraining_count = agetraining_rdd.count()
agetraining_count
agetest_count = agetest_rdd.count()
agetest_count
# Create prediction model
agemodel = DecisionTree.trainClassifier(agetraining_rdd,
                                     numClasses=15,
                                     categoricalFeaturesInfo={
                                        0: 4,
                                        1: 12,
                                        2: 7,
                                        3: 3
                                     })

# Create prediction rdd
tempRDD = agetest_rdd.map(lambda x: x.features)
agepredictions_rdd = agemodel.predict(tempRDD)
agepredictions_rdd.take(40)
# Create rdd with 2 columns, predicted value and real value
agetruth_and_predictions_rdd = agetest_rdd.map(lambda lp: lp.label).zip(agepredictions_rdd)
# Calculate prediction accuracy
accuracy = agetruth_and_predictions_rdd.filter(lambda v_p: v_p[0] == v_p[1]).count() / float(agetest_count)
print('Accuracy =', accuracy)
print(agemodel.toDebugString())
