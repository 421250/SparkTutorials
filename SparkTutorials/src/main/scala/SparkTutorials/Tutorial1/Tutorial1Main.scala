package SparkTutorials.Tutorial1

import org.apache.spark.sql.types.{LongType, StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

object Tutorial1Main  {
  /*
  Spark: The very first step:
  In this tutorial, we will learn the following:
  1. How to create a spark session in local / yarn (Yet Another Resource Negotiator
  2. How to give our application an application name when submitting.
  3. How to read a paraquet/csv/orc file
  4. Learning how to perform simple narrow / wide transformations
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

    println("test")
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

    /* The first 10 rows
      +-----------------+-------------------+-----+
      |DEST_COUNTRY_NAME|ORIGIN_COUNTRY_NAME|count|
      +-----------------+-------------------+-----+
      |             null|               null| null|
      |    United States|            Romania|    1|
      |    United States|            Ireland|  264|
      |    United States|              India|   69|
      |            Egypt|      United States|   24|
      |Equatorial Guinea|      United States|    1|
      |    United States|          Singapore|   25|
      |    United States|            Grenada|   54|
      |       Costa Rica|      United States|  477|
      |          Senegal|      United States|   29|
      +-----------------+-------------------+-----+
      only showing top 10 rows
     */
    println(flightDF.show(10))

    /*
      After you perform the show() operation. We look at how we can form some narrow/wide transformation via sql.
      This done using select.
      +-----------------+-------------------+
      |DEST_COUNTRY_NAME|ORIGIN_COUNTRY_NAME|
      +-----------------+-------------------+
      |DEST_COUNTRY_NAME|ORIGIN_COUNTRY_NAME|
      |    United States|            Romania|
      |    United States|            Ireland|
      |    United States|              India|
      |            Egypt|      United States|
      |Equatorial Guinea|      United States|
      |    United States|          Singapore|
      |    United States|            Grenada|
      |       Costa Rica|      United States|
      |          Senegal|      United States|
      +-----------------+-------------------+
      only showing top 10 rows
     */
    println(flightDF.select("DEST_COUNTRY_NAME","ORIGIN_COUNTRY_NAME").show(10))
    /*
    Or selectExpr
    +-------------------+-----------------+
    |DEST_COUNTRY_NAME_2|DEST_COUNTRY_NAME|
    +-------------------+-----------------+
    |  DEST_COUNTRY_NAME|DEST_COUNTRY_NAME|
    |      United States|    United States|
    |      United States|    United States|
    |      United States|    United States|
    |              Egypt|            Egypt|
    |  Equatorial Guinea|Equatorial Guinea|
    |      United States|    United States|
    |      United States|    United States|
    |         Costa Rica|       Costa Rica|
    |            Senegal|          Senegal|
    +-------------------+-----------------+
    only showing top 10 rows
     */
    println(flightDF.selectExpr("DEST_COUNTRY_NAME as DEST_COUNTRY_NAME_2","DEST_COUNTRY_NAME").show(10))
    /*
    Do some aggregation to count distinct DEST_COUNTRY_NAME
    +---------------------------------+
    |count(DISTINCT DEST_COUNTRY_NAME)|
    +---------------------------------+
    |                              168|
    +---------------------------------+
    */
    println(flightDF.selectExpr("count(distinct(DEST_COUNTRY_NAME))").show(10))
    /*
      Do some aggregation to sum all count given DEST_COUNTRY_NAME
      +---------------------------------+
      |count(DISTINCT DEST_COUNTRY_NAME)|
      +---------------------------------+
      |                              168|
      +---------------------------------+
      */

    /*
    Explore some aggregation function to perform summation
    3 methods
    +-----------------+-----------+
    |DEST_COUNTRY_NAME|SumCounting|
    +-----------------+-----------+
    |             Chad|          1|
    |         Anguilla|        158|
    |           Russia|       1117|
    |         Paraguay|        485|
    |            Yemen|          1|
    |          Senegal|        192|
    |           Sweden|        456|
    |         Kiribati|        152|
    |           Guyana|        258|
    |      Philippines|        799|
    +-----------------+-----------+
    only showing top 10 rows
     */
    println(flightDF.selectExpr("DEST_COUNTRY_NAME","count as SumCount").groupBy("DEST_COUNTRY_NAME"))
    println(flightDF.selectExpr("DEST_COUNTRY_NAME","count as SumCount").groupBy("DEST_COUNTRY_NAME").sum("SumCount").show(10))
    println(flightDF.selectExpr("DEST_COUNTRY_NAME","count as counting").groupBy("DEST_COUNTRY_NAME").agg(expr("sum(counting) as SumCounting")).show(10))
  }




}
