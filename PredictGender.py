# Binary Decision Tree algorithm for predicting the Gender given the Year, Month & Day of Week
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
def genderencode(line):
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
    if ((age >= '16') & (age <= '95')):
        age = float(age)-1
    else: age = 41.0
    if ((month >= '1') & (month <= '12')):
        month = float(month)-1
    else: month = 1.0
    genderfeatures = [y, month, d]
    return LabeledPoint(s, genderfeatures)

# Create labeled points rdd
genderlabeled_points_rdd = dataset_rdd.map(genderencode)
genderlabeled_points_rdd.takeSample(False, 5, 0)
# Create training & test set rdd
gendertraining_rdd, gendertest_rdd = genderlabeled_points_rdd.randomSplit([0.7, 0.3], seed = 0)
gendertraining_count = gendertraining_rdd.count()
gendertraining_count
gendertest_count = gendertest_rdd.count()
gendertest_count
# Create prediction model
gendermodel = DecisionTree.trainClassifier(gendertraining_rdd,
                                     numClasses=3,
                                     categoricalFeaturesInfo={
                                        0: 4,
                                        1: 12,
                                        2: 7,
                                     })

# Create prediction rdd
tempRDD = gendertest_rdd.map(lambda x: x.features)
genderpredictions_rdd = gendermodel.predict(tempRDD)
genderpredictions_rdd.take(40)
# Create rdd with 2 columns, predicted value and real value
gendertruth_and_predictions_rdd = gendertest_rdd.map(lambda lp: lp.label).zip(genderpredictions_rdd)
# Calculate prediction accuracy
accuracy = gendertruth_and_predictions_rdd.filter(lambda v_p: v_p[0] == v_p[1]).count() / float(gendertest_count)
print('Accuracy =', accuracy)
print(gendermodel.toDebugString())
