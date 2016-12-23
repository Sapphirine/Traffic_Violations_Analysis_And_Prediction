from datetime import *
from dateutil.parser import parse
from pyspark.sql.types import *         
from pyspark import SQLContext
sqlContext = SQLContext(sc)

Traffic_rdd = sc.textFile("/Users/Aditi/Desktop/Spark/spark-2.0.1-bin-hadoop2.7/BigDataProject/Traffic_Tickets_Issued__Four_Year_Window.csv")

#print Traffic_rdd.count()

header = Traffic_rdd.first()
schemaString = header.replace('"','')
#print schemaString

fields = [StructField(field_name, StringType(), True) for field_name in schemaString.split(',')]
print len(fields)

fields[2].dataType = FloatType()
fields[3].dataType = FloatType()
fields[5].dataType = FloatType()
print fields

schema = StructType(fields)

TrafficHeader = Traffic_rdd.filter(lambda l: "Violation Charged Code" in l)
TrafficHeader.collect()

TrafficNoHeader = Traffic_rdd.subtract(TrafficHeader)
#print TrafficNoHeader.top(1)

Traffic_temp = TrafficNoHeader.map(lambda k: k.split(",")).map(lambda p: (p[2], p[3], p[5]))
Traffic_temp.saveAsTextFile('labels-and-predictions_three.csv')
print Traffic_temp.top(1)

#Traffic_df = sqlContext.createDataFrame(Traffic_temp, schema)
#print Traffic_df.head(1)
