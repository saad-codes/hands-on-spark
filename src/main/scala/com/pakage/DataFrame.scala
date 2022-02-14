package com.pakage
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types.{IntegerType, StringType, StructType}
import org.apache.spark.sql.functions.{col, expr}
object DataFrame extends App {

  val spark:SparkSession = SparkSession.builder()
    .master("local[5]")
    .appName("SparkByExamples.com")
    .getOrCreate()

//  val data = Seq(
//    Row("James","","Smith","36636","NewYork",3100),
//    Row("Michael","Rose","","40288","California",4300),
//    Row("Robert","","Williams","42114","Florida",1400),
//    Row("Maria","Anne","Jones","39192","Florida",5500),
//    Row("Jen","Mary","Brown","34561","NewYork",3000)
//  )
//
//  val schema = new StructType()
//    .add("firstname",StringType)
//    .add("middlename",StringType)
//    .add("lastname",StringType)
//    .add("id",StringType)
//    .add("location",StringType)
//    .add("salary",IntegerType)
//
//  val df = spark.createDataFrame(
//    spark.sparkContext.parallelize(data),schema)
//  df.printSchema()
//  df.show(false)
//
//  df.drop(df("firstname"))
//    .printSchema()
//
//  df.drop(col("firstname"))
//    .printSchema()
//
//  val df2 = df.drop("firstname")
//  df2.printSchema()
//
//  df.drop("firstname","middlename","lastname")
//    .printSchema()
//
//  val cols = Seq("firstname","middlename","lastname")
//  df.drop(cols:_*)
//    .printSchema()
//

  val data = Seq(("Banana",1000,"USA","A"), ("Carrots",1500,"USA","B"), ("Beans",1600,"USA","C"),
    ("Orange",2000,"USA","B"),("Orange",2000,"USA","A"),("Banana",400,"China","C"),
    ("Carrots",1200,"China","C"),("Beans",1500,"China","C"),("Orange",4000,"China","C"),
    ("Banana",2000,"Canada","B"),("Carrots",2000,"Canada","A"),("Beans",2000,"Mexico","A"))

  import spark.sqlContext.implicits._
  val df = data.toDF("Product","Amount","Country","Category")
  df.show()

  val countries = Seq("USA","China","Canada","Mexico")
  val pivotDF = df.groupBy("Product").pivot("Country", countries).avg("Amount")
  pivotDF.show()

  //unpivot
  val unPivotDF = pivotDF.select($"Product",
    expr("stack(4, 'Canada', Canada, 'China', China, 'Mexico', Mexico,'USA',USA) as (Country,Total)"))
    .where("Total is not null")
  unPivotDF.show()


}

