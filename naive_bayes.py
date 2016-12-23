#NaiveBayes Example.

from __future__ import print_function

import shutil

from pyspark import SparkContext
# $example on$
from pyspark.mllib.classification import NaiveBayes, NaiveBayesModel
from pyspark.mllib.regression import LabeledPoint, LinearRegressionWithSGD, LinearRegressionModel
from pyspark.mllib.util import MLUtils


# $example off$

if __name__ == "__main__":

    sc = SparkContext(appName="PythonNaiveBayesExample")

    # $example on$
    def parsePoint(line):
      values = [float(x) for x in line.replace(',', ' ').split(' ')]
      return LabeledPoint(values[0], values[1:])

    # Load and Parse the data file
    data_1 = sc.textFile("/home/srinidhi/.linuxbrew/Cellar/spark-2.0.1-bin-hadoop2.7/temp.txt")
    data = data_1.map(parsePoint)

   # data = sc.textFile("/home/srinidhi/.linuxbrew/Cellar/spark-2.0.1-bin-hadoop2.7/temp.txt")

    # Split data approximately into training (60%) and test (40%)
    training, test = data.randomSplit([0.6, 0.4])

    # Train a naive Bayes model.
    model = NaiveBayes.train(training, 1.0)

    # Make prediction and test accuracy.
    predictionAndLabel = test.map(lambda p: (model.predict(p.features), p.label))
    accuracy = 1.0 * predictionAndLabel.filter(lambda (x, v): x == v).count() / test.count()
    print('model accuracy {}'.format(accuracy))

    # Save and load model
    output_dir = 'target333/tmp/myNaiveBayesModel'
    shutil.rmtree(output_dir, ignore_errors=True)
    model.save(sc, output_dir)
    sameModel = NaiveBayesModel.load(sc, output_dir)
    predictionAndLabel = test.map(lambda p: (sameModel.predict(p.features), p.label))
    accuracy = 1.0 * predictionAndLabel.filter(lambda (x, v): x == v).count() / test.count()
    print('sameModel accuracy {}'.format(accuracy))

    # $example off$
