# SparkTutorials
For beginners or intermediate developers of Scala Spark 

The main motivation of this tutorial is to allow begineers of Scala Spark to download, compile and run.

# How to start

Navigate to the SparkTutorial folder and mvn package to build. Modify the POM file as you wish.

#Tutorial 1: First Steps 

In this tutorial, we will learn the following:
1. How to create a spark session in local / yarn (Yet Another Resource Negotiator
2. How to give our application an application name when submitting.
3. How to read a paraquet/csv/orc file
4. Learning how to perform simple narrow / wide transformations

#Tutorial 2: UDF,more transformation, and Explain
In this tutorial, we will learn the following:
1. How to use create UDF (User Defined Function):
  We will illustrate how to define UDF and practise casting of data type.
  We will also cover how to transform one row to multiple rows after applying UDF.
2. Further transformation operations: Casting datatypes / Filter Function / Map / flatMap
3. Spark Optimization and Explain Plan

#Tutorial 3: UDAF 
 In this tutorial, we will learn the following:
 1. How to create UDAF,
 2. Walk through the abstract methods / classes that is utilized for UDAF
 
#Tutorial 4: Custom Partitioner
  1. GroupByKey and ReduceByKey incur significant shuffling cost. In this tutorial , we look
  at how to "group" keys together without incurring significant shuffling cost across executors.
  2. Using this tutorial, introduce the use of RDD.
