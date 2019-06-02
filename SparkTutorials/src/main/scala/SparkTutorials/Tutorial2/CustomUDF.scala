package SparkTutorials.Tutorial2

import scala.collection.mutable.{ArrayBuffer, ListBuffer}

/*
We must allow Spark to serialize this class in order to be executed in its executor (worker nodes)

 */
class CustomUDF() extends Serializable {
  /** Raise the base to a power of the exponent, i.e.  base ^ exponent
    *
    *  @param base specify the value of the base
    *  @param exponent specify the exponent, i.e. the power you are trying to raise the base to
    *  @return the result of base ^ exponent
    */
  def power(base:Double,exponent:Double):Double={
    return Math.pow(base,exponent)
  }
  /** Raise the base,x to a power of the exponent, i.e.  base ^ exponent to  the following [x^0,x,x^2]
    *
    *  @param base specify the value of the base
    *  @return the result of [x^0,x,x^2]
    */
  def powerList(base:Double):ArrayBuffer[Double]={
    val listOfValues: ArrayBuffer[Double] = new ArrayBuffer[Double]()
    listOfValues.append(power(base,0))
    listOfValues.append(power(base,1))
    listOfValues.append(power(base,2))
    return listOfValues
  }
}
