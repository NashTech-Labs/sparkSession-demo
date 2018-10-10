# sparks-final-assignment
This example is for implementation of SparkSession
##Apache Spark
##Final Assignment
Run following commands to execute this repo :-
clone it from :
1.git clone https://github.com/knoldus/sparkSession-demo.git
2.cd sparks-final-assignment/
3.sbt clean compile run



##Aim​ :
Complete all the questions given below using Spark Shell or creating an SBT application using
Spark dependencies.
Description of Data​ :


Power plant has a lot of machines. Each machine has a thermostat attached to it. Each thermostat
emits temperature (in o​ ​ C) information along with the timestamp of the temperature recorded.

Eg​ :
Timestamp, Temperature
2010-03-08T17:19:59.000Z, 84.96
2010-03-08T17:20:00.000Z, 88.41
...
...
Note​ : The data will be provided in this repo.

Aim​ :
We need to save data in Parquet file format with following requirements:
1. Saved data should be indexed.
2. Saved data can be retrieved by specific date or time period (span) efficiently (i.e., by
index).
