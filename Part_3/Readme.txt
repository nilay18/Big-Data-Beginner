Steps to execute the program:

As we have written code in python. there are certain steps need to be followed in an order. Steps may vary based on operating system. The steps below are executed with Windows 10 Home.

Prerequisites:-
1) Install spark and setup the environment with python 3.5, jdk 1.8.* and scala.
2) Download winutils and place it in the same folder as spark.
3) Download spark-xml_2.11-0.5.0 / spark-xml_2.12-0.5.0 and place it in spark/jars folder.

Steps:-
There are two ways python code can be executed.

1) Using command line
	a) Setup an environment to execute dataframe api. Enter command pyspark --packages com.databricks:spark-xml_2.11:0.5.0.
	b) execute python code line by line to generate the output.
	c) output files will be genearated as per location given in the code.

2) Step 3 of prerequisite is required to generate output using method 2.
	a) Place jar file in spark/jars folder.
	b) execute python code to generate output.
	c) output files will be genearated as per location given in the code.