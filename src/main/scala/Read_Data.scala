import org.apache.log4j._
import org.apache.spark.sql.{SaveMode, SparkSession}

object Read_Data {
  def main(args: Array[String]) {
    Logger.getLogger("org").setLevel((Level.ERROR))
    val spark : SparkSession = SparkSession.builder
      .appName("test")
      .master("local[2]")
      .getOrCreate()

    val database = "BatchSource"
    val user = "root"
    val password = "mysql1122"
    val connString = "jdbc:mysql://localhost:3306/" + database

    val custDF =( spark.read.format("jdbc")
      .option("url", connString)
      .option("dbtable", "customers")
      .option("user", user)
      .option("password", password)
      .option("driver", "com.mysql.cj.jdbc.Driver")
      .load())

    val ordersDF = (spark.read.format("jdbc")
      .option("url", connString)
      .option("dbtable", "orders")
      .option("user", user)
      .option("password", password)
      .option("driver", "com.mysql.cj.jdbc.Driver")
      .load())

    val deliveryDF = (spark.read.format("jdbc")
      .option("url", connString)
      .option("dbtable", "delivery")
      .option("user", user)
      .option("password", password)
      .option("driver", "com.mysql.cj.jdbc.Driver")
      .load())


    val DF = ordersDF.join(custDF,ordersDF("customerID") ===  custDF("customerID"),"inner").
      join(deliveryDF,deliveryDF("orderID") ===  ordersDF("orderID"),"inner")
      .filter(custDF("customerName")==="ALI" && deliveryDF("status")==="YES")
      .select("customerName","productName","c_city","c_PurchaseDate","deliveryDate","status","amount")

    DF.show()




    DF.write.format("jdbc")
      .option("url","jdbc:mysql://localhost:3306/" + "BatachTarget")
      .option("dbtable", "customerDetail")
      .option("user", "root")
      .option("password", "mysql1122")
      .option("driver", "com.mysql.cj.jdbc.Driver")
      .mode(SaveMode.Overwrite)
      .save()
  }
}