package SparkTutorials.Tutorial4

import org.apache.spark.Partitioner
import org.apache.spark.sql.Row
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types._

/*
To create a custom Partitioner class, we first extends the Partitioner class to inherit all its methods.
We also need to implement the abstract method called getPartitionKey and numPartitions
In this tutorial we will do a simple HashCustomPartitioner
 */

/** CustomPartitioner Class
  *
  *  @param numberOfParitions Number of Partitions you wish to create
  */
class HashCustomPartitioner(numberOfParitions : Integer) extends Partitioner {
  /** Implementing the abstract method called numPartitions
    *
    *  @return the number of partitions specified in the constructor
    */
  def numPartitions():Int={
    return numberOfParitions
  }

  /** Implementing the abstract method called getPartitionKey
    * This is usually done if you intend to get good / uniform / special distribution of the key in the different partitions.
    * Usually, this is done when you have the domain knowledge of the key distribution in order to optimize the partition.
    * By doing so, you can achieve optimal within executor reduceByKey without incurring shuffling.
    *
    *  @param key specify the key of the RDD (key,vaue)
    *  @return the result of the partition that the (key,valuee) will be existed in
    */
  def getPartition(key : Any) : Int={
    return ((key.hashCode() % numberOfParitions) + numberOfParitions) % numberOfParitions
  }


}
