package SparkTutorials.Tutorial3

import org.apache.spark.sql.Row
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types._

import scala.collection.mutable.ArrayBuffer

/*
To create a UDAF, we first extends the UserDefinedAggregateFunction.

Some Object Oriented Programming Concept for those that are unfamiliar with OOP
(Please skip if you familiar):

First UserDefinedAggregateFunction is an abstract class which contains both abstract / non-abstract methods.
We will have to implement all abstract methods. We can choose to override the implementations of non-abstract methods.
To inherit the methods in UserDefinedAggregateFunction, we use the keyword extend.

In UserDefinedAggregateFunction, we have the following abstract methods which requires us to do implementation.
(Reference and acknowledge : Spark: The Definitive Guide : Bill Chambers & Matei Zaharia):

inputSchema represents input arguments as StructType
bufferSchema represents intermediate UDAF results as StructType
dataType represents return type DataType
deterministic boolean value that specifies that this UDAF will return the same result for a given input
initialize allows you to initialize values of an aggregation buffer. Return MutableAggregationBuffer
update how the internal buffer should be update based on a given row
merge describes how 2 aggregation  buffers should be merged
evaluate generate the final results of the aggregation


 */
class Mean() extends UserDefinedAggregateFunction {

  /** returns input arguments as StructType
    *
    */
  def inputSchema():StructType={
    return StructType(StructField("value",DoubleType,false)::Nil)
  }
  /** returns intermediate UDAF results as StructType
    *
    */
  def bufferSchema():StructType={
    return StructType(Array(
      StructField("count",DoubleType,true),
      StructField("values",DoubleType,true)
    ))
  }
  /** returns the return type of the UDAF as DataType
    *
    */
  def dataType():DataType={
    return DoubleType
  }

  /** boolean value that specifies that this UDAF will return the same result for a given input
    * Spark splits the data up and process it seperately. This implies that no matter how spark process
    * and split the data up, the result is independent of its ordering of processing / combination.
    * This affects the optimization of execution plan for the UDAF.
    */
  def deterministic():Boolean={
    return true
  }


  /**  allows you to initialize values of an aggregation buffer. Return MutableAggregationBuffer
    *  In this case, we are setting the initial buffer value as 0 and 0.
    *  Ths first buffer element represents the number of points aggregation
    *  The 2nd buffer element represents the values of the summation.
    */
  def initialize(buffer : MutableAggregationBuffer):Unit={
    buffer(0) = 0d
    buffer(1) = 0d
  }

  /**  how the internal buffer should be update based on a given row
    *  In this case, we are adding 1 to each new row to the number of points that is involved in aggregation
    *  This forms the first buffer element update
    *  The 2nd buffer element update sums each row to the previous summation.
    */

  def update(buffer : MutableAggregationBuffer,input:Row):Unit={
    buffer(0) = buffer.getAs[Double](0) + 1d
    buffer(1) = buffer.getAs[Double](1) + input.getAs[Double](0)
  }


  /**  describes how 2 aggregation buffers should be merged
    */

  def merge(buffer1 : MutableAggregationBuffer,buffer2 : Row):Unit={
    buffer1(0) = buffer1.getAs[Double](0) + buffer2.getAs[Double](0)
    buffer1(1) = buffer1.getAs[Double](1) + buffer2.getAs[Double](1)
  }

  def evaluate(buffer : Row) : Any={
    return buffer.getAs[Double](1) / buffer.getAs[Double](0)
  }


}
