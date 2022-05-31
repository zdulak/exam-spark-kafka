import org.apache.spark.sql.SparkSession
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should

class SparkKafkaAppSpec extends AnyFlatSpec with should.Matchers {
  behavior of "The transformation method"
  it should "return Dataframe(John, DOE, 3) for " in {

    implicit val spark: SparkSession = SparkSession
      .builder()
      .appName(SparkKafkaApp.getClass.getSimpleName)
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._
    import org.apache.spark.sql.functions._

    val input = Seq(("John","Doe",  1,  2)).toDF("name", "surname", "cats", "dogs")
    val expected = Seq(("John", "DOE", 3))

    val actual = SparkKafkaApp.transformation(input).as[(String, String, Int)].collect().toList

    actual should contain theSameElementsAs expected
  }
}
