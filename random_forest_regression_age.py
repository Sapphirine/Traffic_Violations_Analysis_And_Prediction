#Random Forest Regression Model for Predicting Age

from __future__ import print_function

from pyspark import SparkContext
# $example on$
from pyspark.mllib.tree import RandomForest, RandomForestModel
from pyspark.mllib.util import MLUtils
from pyspark.mllib.regression import LabeledPoint, LinearRegressionWithSGD, LinearRegressionModel

# $example off$

if __name__ == "__main__":
    sc = SparkContext(appName="PythonRandomForestRegressionExample")
    # $example on$
    # Load and parse the data file into an RDD of LabeledPoint.
    def parsePoint(line):
      values = [float(x) for x in line.replace(',', ' ').split(' ')]
      return LabeledPoint(values[2], [values[0],values[1]])

    data_1 = sc.textFile("/home/srinidhi/.linuxbrew/Cellar/spark-2.0.1-bin-hadoop2.7/temp.txt")
    data = data_1.map(parsePoint)
   
    # data = MLUtils.loadLibSVMFile(sc, '/home/srinidhi/.linuxbrew/Cellar/spark-2.0.1-bin-hadoop2.7/temp.txt')
    # Split the data into training and test sets (20% held out for testing)
    (trainingData, testData) = data.randomSplit([0.8, 0.2])

    # Train a RandomForest model.
    #  Empty categoricalFeaturesInfo indicates all features are continuous.
    #  Note: Use larger numTrees in practice.
    #  Setting featureSubsetStrategy="auto" lets the algorithm choose.
    model = RandomForest.trainRegressor(trainingData, categoricalFeaturesInfo={},
                                        numTrees=2, featureSubsetStrategy="auto",
                                        impurity='variance', maxDepth=4, maxBins=32)

    # Evaluate model on test instances and compute test error
    predictions = model.predict(testData.map(lambda x: x.features))
    labelsAndPredictions = testData.map(lambda lp: lp.label).zip(predictions)
    testMSE = labelsAndPredictions.map(lambda (v, p): (v - p) * (v - p)).sum() /\
        float(testData.count())
    print('Test Mean Squared Error = ' + str(testMSE))
    print('Learned regression forest model:')
    print(model.toDebugString())
    print('Test Mean Squared Error = ' + str(testMSE))

    # Save and load model
    model.save(sc, "target/tmp/myRandomForestRegressionModel")
    sameModel = RandomForestModel.load(sc, "target/tmp/myRandomForestRegressionModel")
    # $example off$
