package SparkTutorials.Tutorial2

import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.catalyst.encoders.RowEncoder

import scala.collection.mutable.{ArrayBuffer}

object Tutorial2Main  {
  /*
  Spark: More about UDF and some more commonly utilized transformation operations.
  In this tutorial, we will learn the following:
  1. How to use create UDF (User Defined Function):
    We will illustrate how to define UDF and practise casting of data type.
    We will also cover how to transform one row to multiple rows after applying UDF.
  2. Further transformation operations: Casting datatypes / Filter Function
  3. Spark Optimization and Explain Plan
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
    First we will create a UDF. In this exercise, let us create a seperate Class to hold all the UDF methods.
    Let's name the class CustomUDF. Please go to the CustomUDF class for more information.
    We have created a method called power for this portion of the tutorial.
     */

    //Create new customUDF object
    val customUDF : CustomUDF = new CustomUDF
    //Create new udf using the power method
    val powerUDF : UserDefinedFunction = udf(customUDF.power(_:Double,_:Double):Double)
    /* register the UDF with the Spark Session as a UDF called powerFunction.
    Note that the registered name can be different from the method name.
    */
    spark.udf.register("powerFunction",powerUDF)
    /*
    Now we try to raise the column count which is integer type to the power to 3.
    Notice that powerUDF takes in 2 Double input type.
    We shall cast the integer type to double.

    +-----+-------------+
    |count|countToPower3|
    +-----+-------------+
    |    1|          1.0|
    |  264|  1.8399744E7|
    |   69|     328509.0|
    |   24|      13824.0|
    |    1|          1.0|
    |   25|      15625.0|
    |   54|     157464.0|
    |  477| 1.08531333E8|
    |   29|      24389.0|
    +-----+-------------+
    only showing top 10 rows

     */
    println(flightDF.filter("count is not null").selectExpr("count","powerFunction(cast(count as Double),3) as countToPower3").show(10))
    //Refer to the previous line.
    // We further introduce a filter function. All filters are automatically applied at the start.
    // To prove this, lets run the next code.
    /*
    For people that are interested in how is your job getting optimized.
    All Spark SQL/DATAFRAME/DATASET goes through the Spark Catalyst Optimizer
    It contains several stages:
    1. Generating Logical Plan: At this stage, they will only return a set of abstract transformation without checking whether the tables or columns exist.
    2. Resolving Logical Plan: At this stage, a Catalog of tables and dataframe information will be utilized to resolve the logical plan
    3. Optimized Logical Plan: Once resolved, Catalyst's heuristics will perform optimization on the resolved logical plan.
       You can extend the Catalyst rules by including your own heuristics. (By Default, this is turned off. You can turn on by using this property spark.sql.cbo.enabled)
    4. Physical Planning Process: At this stage, cost will be taken into account. The optimizer will  generate several execution plans and compare the cost via table stats.
    5. Finally Execution.
     */
    println(flightDF.selectExpr("count","powerFunction(cast(count as Double),3) as countToPower3").filter("count is not null").explain(true))
    /*
    == Parsed Logical Plan ==
    'Filter isnotnull('count)
    +- Project [count#2L, if ((isnull(cast(count#2L as double)) || isnull(cast(3 as double)))) null else UDF(cast(count#2L as double), cast(3 as double)) AS countTo
    Power3#17]
       +- Relation[DEST_COUNTRY_NAME#0,ORIGIN_COUNTRY_NAME#1,count#2L] csv

    == Analyzed Logical Plan ==
    count: bigint, countToPower3: double
    Filter isnotnull(count#2L)
    +- Project [count#2L, if ((isnull(cast(count#2L as double)) || isnull(cast(3 as double)))) null else UDF(cast(count#2L as double), cast(3 as double)) AS countTo
    Power3#17]
       +- Relation[DEST_COUNTRY_NAME#0,ORIGIN_COUNTRY_NAME#1,count#2L] csv

    == Optimized Logical Plan ==
    Project [count#2L, if (isnull(cast(count#2L as double))) null else UDF(cast(count#2L as double), 3.0) AS countToPower3#17]
    +- Filter isnotnull(count#2L)
       +- Relation[DEST_COUNTRY_NAME#0,ORIGIN_COUNTRY_NAME#1,count#2L] csv

    == Physical Plan ==
    *(1) Project [count#2L, if (isnull(cast(count#2L as double))) null else UDF(cast(count#2L as double), 3.0) AS countToPower3#17]
    +- *(1) Filter isnotnull(count#2L)
       +- *(1) FileScan csv [count#2L] Batched: false, Format: CSV, Location: InMemoryFileIndex[file:/C:/Users/421250/SparkTutorials/Data/flight-data/csv/2010-summa
    ry.csv, fil..., PartitionFilters: [], PushedFilters: [IsNotNull(count)], ReadSchema: struct<count:bigint>
    ()
     */

    /*
    Let's delve into more applications of UDF. Given a single row, you wish to pass the column value into udf and return several rows.
    This is very important in many data engineering application. Let's say the column value contains a rowkey to retrieve several rows.
    This allows us to perform some form of a "hash-join".
    In this example, let us do some simulation. We create a udf that does the following operation:
    1. Given a column value,x, we return the result of x^0 x^1 x^2 as different rows.
     */
    val powerListUDF : UserDefinedFunction = udf(customUDF.powerList(_:Double):ArrayBuffer[Double])
    spark.udf.register("powerListFunction",powerListUDF)
    println(flightDF.filter("count is not null").selectExpr("count","powerListFunction(cast(count as Double)) as countPowerList").show(10))
    /*
    +-----+--------------------+
    |count|      countPowerList|
    +-----+--------------------+
    |    1|     [1.0, 1.0, 1.0]|
    |  264|[1.0, 264.0, 6969...|
    |   69| [1.0, 69.0, 4761.0]|
    |   24|  [1.0, 24.0, 576.0]|
    |    1|     [1.0, 1.0, 1.0]|
    |   25|  [1.0, 25.0, 625.0]|
    |   54| [1.0, 54.0, 2916.0]|
    |  477|[1.0, 477.0, 2275...|
    |   29|  [1.0, 29.0, 841.0]|
    |   44| [1.0, 44.0, 1936.0]|
    +-----+--------------------+
    only showing top 10 rows
    This shows for a given row,  UDF return a list of values which we are interested to transform it into a 3 rows.
    Bill Chambers & Matei Zaharia recommended to use the function called explode.
    But this function will be deprecated in the future. Hence, in this tutorial, we will avoid using it.
    Instead, we will use flatMap.
    For eachRow, we get the ArrayType and iterate via a map function,countPower
    while retaining the rest of the columns, "DEST_COUNTRY_NAME","ORIGIN_COUNTRY_NAME".
    For more information on lambda function, please refer to subsequent tutorials
     */

    import spark.implicits._
    println(flightDF.filter("count is not null").selectExpr("DEST_COUNTRY_NAME","ORIGIN_COUNTRY_NAME","count","powerListFunction(cast(count as Double)) as countPowerList").
      flatMap{ eachRow => eachRow.getAs[ArrayBuffer[Double]]("countPowerList").map{
        eachValue => (eachRow.getAs[String]("DEST_COUNTRY_NAME"),eachRow.getAs[String]("ORIGIN_COUNTRY_NAME"),eachValue)
      }}.selectExpr("_1 as DEST_COUNTRY_NAME","_2 as ORIGIN_COUNTRY_NAME","_3 as countPower").show(10))


    /*
    +-----------------+-------------------+----------+
    |DEST_COUNTRY_NAME|ORIGIN_COUNTRY_NAME|countPower|
    +-----------------+-------------------+----------+
    |    United States|            Romania|       1.0|
    |    United States|            Romania|       1.0|
    |    United States|            Romania|       1.0|
    |    United States|            Ireland|       1.0|
    |    United States|            Ireland|     264.0|
    |    United States|            Ireland|   69696.0|
    |    United States|              India|       1.0|
    |    United States|              India|      69.0|
    |    United States|              India|    4761.0|
    |            Egypt|      United States|       1.0|
    +-----------------+-------------------+----------+
    only showing top 10 rows
     */
  }




}
