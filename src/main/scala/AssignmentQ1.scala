import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{Row, SparkSession}

object AssignmentQ1 extends App{

  Logger.getLogger("org").setLevel(Level.ERROR)
  implicit val spark:SparkSession = SparkSession.builder()
    .master("local[*]")
    .appName("LogFile")
    .getOrCreate()

   val data = Seq(Row(Row("James","","Smith"),19011998,"M",3000),
                    Row(Row("Michael","Rose",""),10111998,"M",20000),
                    Row(Row("Robert","","williams"),22012000,"M",3000),
                    Row(Row("Maria","Anne","Jones"),13011998,"F",11000),
                     Row(Row("Jen","Mary","Brown"),14101998,"F",10000))

  val Schema = Service.NewSchema()
  val data2 = spark.createDataFrame(spark.sparkContext.parallelize(data),Schema)
  data2.printSchema()  // printing the scheme
  data2.show()         // The output dataframe

  //2. Selecting firstname, lastname,salary
  Service.select(data2).show()

  //3.Add Country, department, and age column in the dataframe.
  Service.Adding_Column(data2).show()

  //4.Change the value of salary column.
  Service.Change_col(data2).show()

  //5.Change the data types of DOB and salary to String
  Service.change_datatypes(data2).show()


  //6.Derive new column from salary column
  Service.Derive_Salary(data2).show()

  //7.Rename nested column( Firstname -> firstposition, middlename -> secondposition, lastname -> lastposition)
  Service.Rename_Col(data2).show()

  //8.Filter the name column whose salary in maximum.
  Service.Maximum_Salary(data2).show()

  //9.Drop the department and age column.
  Service.Drop_column(data2).show()

  //10.List out distinct value of dob and salary
  Service.Distinct_values(data2).show()
}
