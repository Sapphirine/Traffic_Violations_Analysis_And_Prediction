#Linear Regression With SGD Example.

from __future__ import print_function

from pyspark import SparkContext
# $example on$
from pyspark.mllib.regression import LabeledPoint, LinearRegressionWithSGD, LinearRegressionModel
# $example off$

if __name__ == "__main__":

    sc = SparkContext(appName="PythonLinearRegressionWithSGDExample")

    # $example on$
    # Load and parse the data
    def parsePoint(line):
        values = [float(x) for x in line.replace(',', ' ').split(' ')]
        return LabeledPoint(values[0], values[1:])

    data = sc.textFile("/home/srinidhi/.linuxbrew/Cellar/spark-2.0.1-bin-hadoop2.7/temp.txt")
    parsedData = data.map(parsePoint)

    # Build the model
    # 100 iterations with a step size of 0.00000001
    model = LinearRegressionWithSGD.train(parsedData, iterations=100, step=0.00000001)

    # Evaluate the model on training data
    valuesAndPreds = parsedData.map(lambda p: (p.label, model.predict(p.features)))
    MSE = valuesAndPreds \
        .map(lambda (v, p): (v - p)**2) \
        .reduce(lambda x, y: x + y) / valuesAndPreds.count()
    print("Mean Squared Error = " + str(MSE))

    # Save and load model
    model.save(sc, "target/tmp/pythonLinearRegressionWithSGDModel")
    sameModel = LinearRegressionModel.load(sc, "target/tmp/pythonLinearRegressionWithSGDModel")
    # $example off$
