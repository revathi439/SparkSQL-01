import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.scalatest.funsuite.AnyFunSuite

class TestAssignmentQ1 extends AnyFunSuite{
  Logger.getLogger("org").setLevel(Level.ERROR)
  implicit val spark: SparkSession = SparkSession.builder()
    .master("local[*]")
    .appName("LogFile")
    .getOrCreate()

  val input = Seq(Row(Row("Revathi","","Poluparthi"), 13012000, "F", 30000),
    Row(Row("Rama","krishna","S"), 12031998, "M", 50000),
    Row(Row("Krishna","","Royal"), 14042000, "M", 40000),
  Row(Row("Kranthi","sai","K"), 15042000, "F", 40000))

  //Solution 1
  val schema: StructType = Service.NewSchema()
  val dataF: DataFrame = spark.createDataFrame(spark.sparkContext.parallelize(input), schema)

  //Solution 2
  import spark.implicits._
  assert(Service.select(dataF).collect().toList===List(("Revathi","Poluparthi",30000),("Rama","S",50000),("Krishna","Royal",40000),("Kranthi","K",40000)).toDF.collect().toList)

  //Solution 3
  assert(Service.Adding_Column(dataF).collect().toList===List((("Revathi","","Poluparthi"),13012000,"F",30000,"India","Data_engineer",22), (("Rama","krishna","S"),12031998,"M",50000,"India","Data_engineer",22), (("Krishna","","Royal"),14042000,"M",40000,"India","Data_engineer",22), (("Kranthi","sai","K"),15042000,"F",40000,"India","Data_engineer",22)).toDF.collect().toList)

  //Solution 4
  assert(Service.Change_col(dataF).collect().toList===List((60000),(100000),(80000),(80000)).toDF.collect().toList)

  //Solution 5
  assert(Service.change_datatypes(dataF).collect().toList===List((("Revathi","","Poluparthi"),"13012000","F","30000"),(("Rama","krishna","S"),"12031998","M","50000"),(("Krishna","","Royal"),"14042000","M","40000"),(("Kranthi","sai","K"),"15042000","F","40000")).toDF.collect().toList)

  //Solution 6
  assert(Service.Derive_Salary(dataF).collect().toList===List((("Revathi","","Poluparthi"),13012000,"F",30000,51000),(("Rama","krishna","S"),12031998,"M",50000,85000),(("Krishna","","Royal"),14042000,"M",40000,68000),(("Kranthi","sai","K"),15042000,"F",40000,68000)).toDF.collect().toList)

  //Solution 7
  assert(Service.Rename_Col(dataF).collect().toList===List(("Revathi","","Poluparthi"),("Rama","krishna","S"),("Krishna","","Royal"),("Kranthi","sai","K")).toDF.collect().toList)

  //Solution 8
  assert(Service.Maximum_Salary(dataF).collect().toList===List((("Rama","krishna","S"),12031998,"M",50000)).toDF.collect().toList)

  //Solution 9
  assert(Service.Drop_column(dataF).collect().toList===List((("Revathi","","Poluparthi"),13012000,"F",30000),(("Rama","krishna","S"),12031998,"M",50000),(("Krishna","","Royal"),14042000,"M",40000),(("Kranthi","sai","K"),15042000,"F",40000)).toDF.collect().toList)

  //Solution 10
  assert(Service.Distinct_values(dataF).collect().toList===List((("Rama","krishna","S"),12031998,"M",50000),(("Revathi","","Poluparthi"),13012000,"F",30000),(("Krishna","","Royal"),14042000,"M",40000),(("Kranthi","sai","K"),15042000,"F",40000)).toDF.collect().toList)
}
