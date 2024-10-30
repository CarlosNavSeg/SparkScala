package com.sundogsoftware.spark

import org.apache.spark._
import org.apache.log4j._

object TotalAmountSpentCarlos {

  def getCustomerAmountPairs(customer: String): (String,Float) = {
    val fields = customer.split(",")
    val expenditure = fields(2).toFloat
    val customerId = fields(0)
    (customerId, expenditure)
  }

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)

    val sc = new SparkContext("local[*]", "MinTemperatures")

    val lines = sc.textFile("data/customer-orders.csv")

    val customerTuples = lines.map(getCustomerAmountPairs)

    val amountSpentByCostumer = customerTuples.reduceByKey((x,y) => x + y )

    val amountSpentByCustomerSortedValue = amountSpentByCostumer.map(x => (x._2, x._1)).sortByKey()

    val results = amountSpentByCustomerSortedValue.collect()

    for (result <- results.reverse) {
      val customer = result._2
      val amount = result._1
      println(s"$customer spent: $amount")
    }
  }
}
