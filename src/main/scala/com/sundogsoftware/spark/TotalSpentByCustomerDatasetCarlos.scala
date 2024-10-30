package com.sundogsoftware.spark

import com.sundogsoftware.spark.MinTemperaturesDataset.Temperature
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.round
import org.apache.spark.sql.functions.sum
import org.apache.spark.sql.types.{DoubleType, IntegerType, StructType}

object TotalSpentByCustomerDatasetCarlos {

  case class Customer(customer_id: Int, product_id: Int, amount_spent: Double)
  def main(args: Array[String]) {

    // Set the log level to only print errors
    Logger.getLogger("org").setLevel(Level.ERROR)

    // Create a SparkSession using every core of the local machine
    val spark = SparkSession
      .builder
      .appName("MinTemperatures")
      .master("local[*]")
      .getOrCreate()

    val customerSchema = new StructType()
      .add("customer_id", IntegerType, nullable = true)
      .add("product_id", IntegerType, nullable = true)
      .add("amount_spent", DoubleType, nullable = true)

    // Read the file as dataset
    import spark.implicits._
    val ds = spark.read
      .schema(customerSchema)
      .csv("data/customer-orders.csv")
      .as[Customer]

    // Filter out all but TMIN entries
    val customerSpent = ds.select("customer_id", "amount_spent")

    val customerSpentTotal = customerSpent
      .groupBy("customer_id")
      .agg(round(sum("amount_spent"), 2).alias("total_spent"))

    val customerSpentTotalSorted = customerSpentTotal
      .sort("total_spent")

    val results = customerSpentTotalSorted.collect()

    for (result <- results) {
      val customer = result(0)
      val spentTotal = result(1).asInstanceOf[Double]
      val formattedSpent = f"$spentTotal%.2f €"
      println(s"$customer spent in total: $formattedSpent")
    }

  }
}
