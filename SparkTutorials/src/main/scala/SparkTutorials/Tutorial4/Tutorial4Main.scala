package SparkTutorials.Tutorial4

import org.apache.spark.HashPartitioner
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, SparkSession}

object Tutorial4Main  {
  /*
  Spark: RDD and Custom Partitioner
  1. GroupByKey and ReduceByKey incur significant shuffling cost. In this tutorial , we look
  at how to "group" keys together without incurring significant shuffling cost across executors.
  2. Using this tutorial, introduce the use of RDD.
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
    Convert Spark DataFrame to RDD
     */
    System.out.println(flightDF.filter("DEST_COUNTRY_NAME is not null").rdd
      .keyBy(row=>row(1).asInstanceOf[String]).partitionBy(new HashCustomPartitioner(10)).take(10))

  }




}
