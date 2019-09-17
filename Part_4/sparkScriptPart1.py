import sys
from pyspark.sql.functions import *
from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.sql.types import *
from pyspark.context import SparkContext
from pyspark.sql.session import SparkSession
from pyspark.sql import SQLContext

#directory path for spark look up
inputPath = "C:\SparkCourse\Assignment4"

def main():
    #convert dsstream into rdd and process rdd using pyspark sql
    def process(time, rdd):
        #if rdd is created, process it
        if(rdd.count() > 0):
            df = sqlContext.createDataFrame(rdd)
            df.registerTempTable("myCounts")
            print(rdd.count())
            df.printSchema() #print schema
            print(sqlContext.sql("select * from myCounts").count()) #count number of rows in the rdd
            print(sqlContext.sql("select max(_2) from myCounts").collect()) #max value of the rdd 
            print(sqlContext.sql("select avg(_2) from myCounts").collect()) #average value of the rdd
            print(sqlContext.sql("select sum(_2) from myCounts").collect()) #sum of the rdd
        else:
            print("empty rdd")

    sc = SparkContext(appName="PysparkStreaming")
    ssc = StreamingContext(sc, 60)   #Streaming will execute in each 60 seconds
    sqlContext = SQLContext(sc)
    lines = ssc.textFileStream(inputPath)
    counts = lines.flatMap(lambda line: line.split(" ")).map(lambda x: (1, int(x))).reduceByKey(lambda a, b: a + b) #sum of the dOctet
    counts.pprint()
    avgs = lines.flatMap(lambda line: line.split(" ")).map(lambda x: (1, int(x)))
    avgs.foreachRDD(process)

    ssc.start()
    ssc.awaitTermination()

if __name__ == "__main__":
    main()