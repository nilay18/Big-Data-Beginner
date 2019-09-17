-Requires Spark 2.2+.
-Python environment (Canopy), JDK (1.8.0_211 on local system), Spark 2.2+ and winutils(Windows binaries for Hadoop versions)
-StreamingContext is the extension to Spark Core API which will be required to process streaming data.


For Part 1:-------------------------------
Steps for Execution:
1) Run the UDPdataCapture.py (It reads UDP generates and create file)
2) Also run the sparkScriptPart1.py (It uses Spark Stream APIs for processing data present in files simultaneously)
3) Run the Jar file with the below command:
	java -jar data-generator.jar --destIPaddress 127.0.0.1 --destPortNumber 9876 --transmissionTime 30 --transmissionRate 50000


PS: 1, 2 and 3 are executed in parallel.
Output text file will be generated after 30 minutes.

For Part 2:-------------------------------
Steps for Execution:
1) Run UDPdataCapturePart2.py
2) Run scriptPart2.py
3) Run the Jar file with the below command:
	java -jar data-generator.jar --destIPaddress 127.0.0.1 --destPortNumber 9876 --transmissionTime 30 --transmissionRate 50000

PS: 1, 2 and 3 are executed in parallel.
Output text file will be generated after 30 minutes.