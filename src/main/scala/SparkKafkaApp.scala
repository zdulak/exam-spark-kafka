import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.types.{IntegerType, StringType, StructType}
import scala.concurrent.duration.DurationInt
import org.apache.spark.sql.SparkSession

object SparkKafkaApp {
  def main(args: Array[String]): Unit = {
    val topic = if (args.length > 0 ) args(0) else "input"

    implicit val spark: SparkSession = SparkSession
      .builder()
      .appName(SparkKafkaApp.getClass.getSimpleName)
//      .master("local[*]")
      .getOrCreate()

    import spark.implicits._
    import org.apache.spark.sql.functions._

    val schema = new StructType()
      .add("name", StringType, true)
      .add("surname", StringType, true)
      .add("cats", IntegerType, true)
      .add("dogs", IntegerType, true)

//    Dataframe with our data casted from json to columns
    val inputDf = spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("subscribe", topic)
      .load()
      .select($"value".cast("string"))
      .select(from_json($"value", schema).as("value"))
      .select($"value.*")

//    Our transformations
    val df = transformation(inputDf)

//    Writing to parquet on hdfs
    val query = df
      .writeStream
      .format("parquet")
      .outputMode("append")
      .option("path", "hdfs://localhost:9000/output/")
      .option("checkpointLocation", "checkpoint")
      .trigger(Trigger.ProcessingTime(1.seconds))
      .start()

    query.awaitTermination()

//    For testing
//    val queryTest = df
//      .writeStream
//      .format("console")
//      .option("truncate", false)
//      .start()
//
//    queryTest.awaitTermination()
  }

  def transformation(inputDf: DataFrame)(implicit spark: SparkSession): DataFrame = {
    import spark.implicits._
    import org.apache.spark.sql.functions._
    inputDf
      .withColumn("surname", upper($"surname"))
      .withColumn("animals", $"dogs" + $"cats")
      .drop("cats", "dogs")
  }

}
