import sys
from pyspark.sql.functions import *
from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.sql.types import *
from pyspark.context import SparkContext
from pyspark.sql.session import SparkSession
from pyspark.sql import SQLContext

inputPath = "C:\SparkCourse\Assignment4\merge" #directory to look for picking up the data by spark

def main():
    def process(time, rdd):
        if(rdd.count() > 0):
            df = sqlContext.createDataFrame(rdd)
            df.registerTempTable("myips")
            df.printSchema()
            print(sqlContext.sql("select _2, COUNT(*) from myips group by _2 having COUNT(*) > 1").collect()) #collect sourceip,destinationip having count > 1
        else:
            print("empty rdd")
    sc = SparkContext(appName="PysparkStreaming")
    ssc = StreamingContext(sc, 60)   #Streaming will execute in each 3 seconds
    sqlContext = SQLContext(sc)
    lines = ssc.textFileStream(inputPath)
    avgs = lines.flatMap(lambda line: line.split(" ")).map(lambda x: (1, x)) #pass sourceip,destination ip in the rdd
    avgs.foreachRDD(process) #process RDD

    ssc.start()
    ssc.awaitTermination()

if __name__ == "__main__":
    main()