from pyspark import SparkContext,SparkConf
from pyspark.sql import SQLContext
import time

conf = SparkConf().setMaster("local").setAppName("textMining")
sc = SparkContext(conf = conf)
sqlContext = SQLContext(sc)

#Read data from the wiki dump. dataset.xml is a wiki dump of 32GB file.
print("Reading data...")
df = sqlContext.read.format('com.databricks.spark.xml').options(rootTag='page').options(rowTag='page').load('file:///SparkCourse/dataset.xml')

#Schema of the loaded data
df.printSchema()

#Register schema as a table
df.registerTempTable("XMLTABLE")

#query 1 using collect method
start_time = time.time() #assign current time to start_time variable
print("Executing query 1.................")
print(sqlContext.sql("select COUNT(revision.minor) from XMLTABLE where revision.minor=''").collect())
print("--- %s seconds --- for query 1" % (time.time() - start_time)) #print time taken to generate and print minor count

#query 2 using collect method
start_time = time.time() #assign current time to start_time variable
print("Executing query 2.................")
output_data = sqlContext.sql("select id,title from XMLTABLE where (LENGTH(revision.text._VALUE) - LENGTH(REPLACE(revision.text._VALUE,'http','')))/4 < 6").collect()
print("--- %s seconds --- for query 2" % (time.time() - start_time)) #print time taken to process the data
#write output in query2_32GB.txt file 
with open('query2_32GB.txt', 'w', encoding='utf-8') as f:
    f.write(str(output_data))

#query 3 using collect method
start_time = time.time() #assign current time to start_time variable
print("Executing query 3.................")
output_data = sqlContext.sql("select revision.contributor.id AS `Contributor id`, revision.contributor.username AS `Contributor Name` from XMLTABLE where revision.contributor.id in (select revision.contributor.id from XMLTABLE group by revision.contributor.id having COUNT(revision.contributor.id)>1) order by revision.contributor.id,revision.timestamp desc").collect()
print("--- %s seconds --- for query 3" % (time.time() - start_time)) #print time taken to process the data
#write output in query21_32GB.txt file
with open('query3_32GB.txt', 'w', encoding='utf-8') as f:
    f.write(str(output_data))
