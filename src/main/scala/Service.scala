import org.apache.spark.sql.{DataFrame}
import org.apache.spark.sql.functions.{col, lit}
import org.apache.spark.sql.types.{IntegerType, StringType, StructType}


object Service {

  //1.Create a Data Frame with name, firstname, middlename, lastname, dob, gender and salary fields.
  def NewSchema() : StructType ={
    new StructType()
    .add("Name", new StructType().add("Firstname", StringType).add("Middilename", StringType).add("Lastname", StringType))
    .add("DOB", IntegerType)
    .add("Gender", StringType)
    .add("Salary", IntegerType)
}

  //2.Select firstname, lastname and salary from Dataframe.
  def select(dataDF:DataFrame):DataFrame = {
    val schema = dataDF
    schema.select(col("Name.Firstname"),
      col("Name.Lastname"),
      col("Salary"))
  }

  //3.Add Country, department, and age column in the dataframe.
  //Here we can use select, withColumn to add new Column
  def Adding_Column(dataDF:DataFrame):DataFrame={
   val new_column = dataDF
    new_column.withColumn("Country",lit("India"))
      .withColumn("department",lit("Data_engineer"))
      .withColumn("Age",lit(22))
  }

 //4. Change the value of salary column.
  def Change_col(dataDF:DataFrame):DataFrame={
    val chan_col = dataDF
    chan_col.select(col("Salary").as("Salary")*2)
  }

  //5.Change the data types of DOB and salary to String
  def change_datatypes(dataDF:DataFrame):DataFrame= {
    val chan_datatypes = dataDF
    dataDF.withColumn("Salary", col("Salary").cast("String"))
      .withColumn("DOB", col("DOB").cast("String"))
  }

  //6.Derive new column from salary column.
  def Derive_Salary(dataDF:DataFrame):DataFrame={
  val derive_sal= dataDF
  derive_sal.withColumn("Derived_Salary",col("Salary")*1.7)
}

  //7.Rename nested column( Firstname -> firstposition, middlename -> secondposition, lastname -> lastposition)
  def Rename_Col(dataDF:DataFrame):DataFrame={
    val Rename = dataDF
    Rename.select(col("Name.Firstname").as("firstposition"),col("Name.Middilename").as("middileposition")
      ,col("Name.Lastname").as("lastposition"))
  }

  //8.Filter the name column whose salary in maximum.
  def Maximum_Salary(dataDF:DataFrame):DataFrame= {
      val max_Sal = dataDF.select("Salary").collect()
        .toList.flatMap(_.toSeq.asInstanceOf[Seq[Int]]).max
    dataDF.filter(dataDF("Salary")===max_Sal)
  }

  //9.Drop the department and age column
  //Here  created new_columns and I dropped the columns
  def Drop_column(dataDF:DataFrame):DataFrame={
    dataDF.withColumn("department",lit("Data_engineer"))
      .withColumn("Age",lit(22))
    dataDF.drop("department","Age")

  }

  //10.List out distinct value of dob and salary
  def Distinct_values(dataDF:DataFrame):DataFrame={
  dataDF.dropDuplicates("DOB","Salary")
  }
}
