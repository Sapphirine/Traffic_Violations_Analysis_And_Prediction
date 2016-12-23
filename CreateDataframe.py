# We first want to create a dataframe that contains our dataset
from pyspark.sql import SQLContext  # Import SQL modules
sqlContext = SQLContext(sc)
# Load the NY state violations dataset in an rdd and map the columns to indices
raw_rdd = sc.textFile("/Users/GSCHARITOS/Project/Traffic_Tickets_Issued__Four_Year_Window.csv")\
    .map(lambda line: line.split(","))
# Create o dataframe from an the rdd and name it's columns accordingly
raw_df = raw_rdd.toDF(['Violation Charged Code','Violation Description','Violation Year','Violation Month','Violation Day of Week','Age at Violation','Gender','State of License','Police Agency','Court','Source'])
raw_df.show()

