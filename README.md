# hadoop-emulator
A Java based hadoop emulator for parallelization for showing map-shuffle-reduce functionality
GIT URL: https://github.com/maymonms/hadoop-emulator.git

The latest version is available in taskb branck. Not yet merged it back to main.

##Requirements
Java 1.8 or above
Maven

## Description
MapReduce is a distributed programming paradigm that has revolutionized the way big data is
processed with parallel processing, fault tolerance, high availability and resilience. During the
practicals we have already seen how Hadoop can be used to process large datasets in
distributed computing environments. In this project, we develop a MapReduce-like executable
prototype to determine the passenger(s) having had the highest number of flights.

Input file is located at: resources\

## Compiling and Deployment 

1. If you are using eclipse, use the following maven command for setup
mvn eclipse:eclipse

2. Compile and create jar(This step is sufficient to setup and deploy)
mvn clean install  
The above command will make sure that all the required dependancies are installed and deployed. When running for the first time, it might take more time to download and setup the environment. 
We have two version of jars created. 
	-bigdata-0.0.1-SNAPSHOT.jar 
	-bigdata.jar
bigdata-0.0.1-SNAPSHOT.jar is a lighter version of the project, which is intented for hadoop deployment. 


## Usage

java -cp target\bigdata.jar uk.ac.reading.compsci.csmbd21.cw.HadoopEmulator

OR

java -jar target\bigdata.jar


##  Configuration
We have provided the support for customizations and configurations in the file resources\config.properties
You are encouraged to edit this file and re-run the HadoopEmulator.

The logging configurations are provided in resources\logback.properties


