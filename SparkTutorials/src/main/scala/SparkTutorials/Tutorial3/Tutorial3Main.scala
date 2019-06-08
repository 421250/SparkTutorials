package SparkTutorials.Tutorial3

import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.collection.mutable.ArrayBuffer

object Tutorial3Main  {
  /*
  Spark: User Defined Aggregate Functions
   In this tutorial, we will learn the following:
   1. How to create UDAF,
   2. Walk through the abstract methods / classes that is utilized for UDAF
   */
  //Define the applicationName
  final val applicationName : String = "SparkSessionExample"
  //Set master to local implies running Spark locally with one worker thread. In most cases, we are setting to YARN.
  final val setMaster : String = "local"
  //Set file location of file that you wish to read. In this tutorial, we are going to use the flight data.
  final val fileLocation : String = "C:\\Users\\421250\\SparkTutorials\\Data\\flight-data\\csv\\*"

  def main(args: Array[String]): Unit = {

    //Create Spark Session. Further options/configuration can be provided
    // via the command line during the launching of Spark jobs,
    // i.e. specifying number of executors and etc.

    val spark: SparkSession = SparkSession.builder().master(setMaster).appName(applicationName).getOrCreate()

    /*
  Now we can learn to read the files from the HDFS into Spark. We will only use csv for demostration.
  We also ignore the header option by setting to false. This implies that will need to provide the
  header schema for the dataframe
   */
    // define data frame schema
    val flightSchema : StructType = StructType(Array(
      StructField("DEST_COUNTRY_NAME",StringType,true),
      StructField("ORIGIN_COUNTRY_NAME",StringType,true),
      StructField("count",LongType,true)
    ))



    val flightDF: DataFrame = spark.read.format("csv").option("header", "false").schema(flightSchema).load(path = fileLocation)

    /*
    First we will create a UDAF. In this exercise, let us create a seperate Class to hold all the UDF methods.
    Let's name the class Mean. Please go to the Mean class for more information.
    The exact implementation please refer to Mean.

    +-----------------+------------------------------+
    |DEST_COUNTRY_NAME|aggMean(CAST(count AS DOUBLE))|
    +-----------------+------------------------------+
    |             Chad|                           1.0|
    |         Anguilla|            26.333333333333332|
    |           Russia|            186.16666666666666|
    |         Paraguay|             80.83333333333333|
    |            Yemen|                           1.0|
    |          Senegal|                          32.0|
    |           Sweden|                          76.0|
    |         Kiribati|            25.333333333333332|
    |           Guyana|                          43.0|
    |      Philippines|            133.16666666666666|
    |         Djibouti|                           1.0|
    |         Malaysia|                           1.8|
    |        Singapore|                          20.4|
    |             Fiji|                          35.5|
    |           Turkey|            100.16666666666667|
    |           Malawi|                           1.0|
    |             Iraq|                           1.0|
    |          Germany|            1416.8333333333333|
    |      Afghanistan|                           8.0|
    |           Jordan|            52.666666666666664|
    +-----------------+------------------------------+
    only showing top 20 rows
     */

    val mean:Mean = new Mean()
    spark.udf.register("aggMean",mean)
    println(flightDF.filter("count is not null").groupBy("DEST_COUNTRY_NAME")
        .agg(expr("aggMean(cast(count as Double))")).show())

  }




}
