import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object Day18App {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("Day18-Joins")
      .master("local[*]")
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    import spark.implicits._

    val customers = Seq(
      ("C1", "Ali"),
      ("C2", "Rahul"),
      ("C3", "Sara")
    ).toDF("customer_id", "customer_name")

    val orders = Seq(
      ("O1", "C1", 1000),
      ("O2", "C1", 2000),
      ("O3", "C2", 1500),
      ("O4", "C4", 3000)
    ).toDF("order_id", "customer_id", "amount")

    println("INNER JOIN")
    customers.join(orders, Seq("customer_id"), "inner").show()

    println("LEFT JOIN")
    customers.join(orders, Seq("customer_id"), "left").show()

    println("RIGHT JOIN")
    customers.join(orders, Seq("customer_id"), "right").show()

    println("FULL JOIN")
    customers.join(orders, Seq("customer_id"), "full").show()

    println("Null handling:")
    customers
      .join(orders, Seq("customer_id"), "left")
      .withColumn("amount", coalesce($"amount", lit(0)))
      .show()

    spark.stop()
  }
}
