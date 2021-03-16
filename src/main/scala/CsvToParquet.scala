import org.apache.log4j._
import org.apache.spark.sql._
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import scala.io.Source
import org.scalactic.

object CsvToParquet extends App {
// set log level to ERROR
  Logger.getLogger("org").setLevel(Level.ERROR)
  val spark = SparkSession.builder().master("local[*]").appName("CSVToParquet").getOrCreate()
  //val inputFilePath=args(0)
  //val outputFilePath=args(1)
  val inputFilePath="/Users/capgemini/Downloads/data_eng_test 2/data_eng_sample_data.txt"
  val outputFilePath="/Users/capgemini/Downloads/data_eng_test 2/output/"

  def createData(path: String) = {
/** Read Input File Path
 *  Split lines based on delimeter #@#@#
 *  Split row using ~ as delimeter
 *  create an rdd[Row]
 *  Convert rdd - DF
 *  Modify columns based on schema.txt
 * */
    val df = spark.read.textFile(inputFilePath).rdd
    val data = df.flatMap(line => line.split("#@#@#")).map(_.split("'~'")).map(attributes => Row.fromSeq(attributes))

    // Convert rdd to Dataframe
    val schema = StructType(Source.fromFile(path).getLines().toList.map(x => StructField(x.split("\\s+")(0),StringType , true)))
    val final_df=spark.createDataFrame(data,schema)

    // Contains mapping for column type
    val mapper = Source.fromFile(path).getLines().toList.map(x => (x.split("\\s+")(0),getType(x.split("\\s+")(1)))).toMap

    // transform column based on given type
    final_df.columns.foreach(col=>castColumnTo( final_df, col, mapper(col) ))

    // Write parquet to output directory
    final_df.write.parquet(outputFilePath)

  }

  def castColumnTo( df: DataFrame, cn: String, typeto: String ) : DataFrame = {
    df.withColumn(cn+"Tmp", df(cn).cast(typeto))
      .drop(cn)
      .withColumnRenamed(cn+"Tmp", cn)
  }

  def getType(raw: String): String = {
    raw match {
      case "String" => "string"
      case "Float" => "float"
      case "Integer" => "integer"
      case "Date" => "date"
      case _ => "string"
    }
  }

/// create Parqet dataset
  createData("src/main/files/schema.txt")


  /// test datasets for equality
//  class test extends FunSuite with DataFrameSuiteBase {
//    test("simple test") {
//      val sqlCtx = sqlContext
//      import sqlCtx.implicits._
//
//      val input1 = sc.parallelize(List(1, 2, 3)).toDF
//      assertDataFrameEquals(input1, input1) // equal
//
//      val input2 = sc.parallelize(List(4, 5, 6)).toDF
//      intercept[org.scalatest.exceptions.TestFailedException] {
//        assertDataFrameEquals(input1, input2) // not equal
//      }
//    }
//  }

  spark.close()

}
