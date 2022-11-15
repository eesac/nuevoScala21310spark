
//import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
//import org.apache.log4j.{Level, Logger}
//import org.apache.spark
//import org.apache.spark.sql.{DataFrame, SparkSession}
//
//import org.apache.spark
//import org.apache.spark.{SparkConf, SparkContext}
//import org.apache.spark.sql.SparkSession
//import org.apache.spark.sql.functions.{explode, split}
//import com_miTestScala.miClasseScalass



object Main {
  def main(args: Array[String]): Unit = {
//    println("Hello world!")

    val spark = SparkSession.builder().appName("mi sesion").config("spark.some.config.option","some-value").getOrCreate()
    val df_p = spark.read.json("C:\\\\Users\\\\Consultant\\\\Downloads\\\\Properties.json")
    val df_a = spark.read.textFile("C:\\\\Users\\\\Consultant\\\\Downloads\\\\Amenities.txt")
    df_p.show()

    val prop_columns = Seq("property_id","active","discovered_dt")
    val amen_columns = Seq("field","type")

    import spark.implicits._
    df_p.printSchema()
    df_a.select($"field",$"type").show()

// into a data frame    .option("numPartitions", 10)\
    import spark.sqlContext.implicits._
    val dfr_p = df_p.toDF(prop_columns:_*)
    val dfr_a = df_a.toDF(prop_columns:_*)

//    ESCRIBE EN PARQUT
    dfr_p.write.parquet(("C:\\\\Users\\\\Consultant\\\\Downloads\\\\ParquetTables\\\\properties.parquet"))
    dfr_a.write.parquet(("C:\\\\Users\\\\Consultant\\\\Downloads\\\\ParquetTables\\\\amenities.parquet"))
//  PARTICIONA ANTES DE ENVIAR A PARQUET
    dfr_p.write.partitionBy("active","discovered_dt").parquet("C:\\\\Users\\\\Consultant\\\\Downloads\\\\ParquetTables\\\\properties.parquet")
//    dfr_p.write.partitionBy("active","discovered_dt").parquet("C:\\\\Users\\\\Consultant\\\\Downloads\\\\ParquetTables\\\\amenities.parquet")

//JOB 2
//    CREA DATA FRAME DESDE PARQUET TABLES
    val DF_fromParqto_P = spark.read.parquet("C:\\\\Users\\\\Consultant\\\\Downloads\\\\ParquetTables\\\\properties.parquet")
    val DF_fromParqto_A = spark.read.parquet("C:\\\\Users\\\\Consultant\\\\Downloads\\\\ParquetTables\\\\amenities.parquet")
// CREA TABLA PARA LEER Y FILTRA LOS PROPERTIES ACTIVOS
    DF_fromParqto_P.createOrReplaceTempView("PropertiesParquetTable")
    val ReadPropParkSQL = spark.sql("select * from PropertiesParquetTable where active is true")

//    ?? subquery to get amenities with same id

//read a specific parquet partition
//    val DF_fromParqto_P = spark.read.parquet(("C:\\\\Users\\\\Consultant\\\\Downloads\\\\ParquetTables\\\\properties.parquet\\\\actives=true"))
    val DF_parqtoCSV = spark.read.format("parquet")
  .load(("C:\\\\Users\\\\Consultant\\\\Downloads\\\\ParquetTables\\\\properties.parquet"))
    DF_parqtoCSV.show()
    DF_parqtoCSV.printSchema()

    DF_parqtoCSV.write.option("header","true").csv(("C:\\\\Users\\\\Consultant\\\\Downloads\\\\Output_csv\\\\properties.csv"))


//    val data =
//    [
//    ('James','Smith','1991 - 04 - 01',' M', 3000),
//    ('Michael','Rose','','2000 - 05 - 19',' M', 4000),
//    ('Robert','Williams','1978 - 09 - 05',' M', 4000),
//    ('Maria',' Anne ',' Jones ','1967 - 12 - 01',' F', 4000),
//    ('Jen',' Mary ',' Brown ','1980 - 02 - 17',' F', -1)
//    ]

//    val data = [
//    ("James", "Smith", "1991-04-01", "M", 3000)
//    ,("Michael", "Rose", "", "2000-05-19", "M", 4000)
//    ,("Robert", "Williams", "1978-09-05", "M", 4000)
//    ,("Maria", "Anne", "Jones", "1967-12-01", "F", 4000)
//    ,("Jen", "Mary", "Brown", "1980-02-17", "F", -1)
//    ]
//
//    val columns = ["firstname", "middlename", "lastname", "dob", "gender", "salary"]
//    val df = spark.createDataFrame(data = data, schema = columns)
//    val wordsDF = df.select(explode(split(df("value")," ")).alias("word"))
//    val count = wordsDF.groupBy("word").count()

    //    EJEMPLO DE SESION SPARK CONTEXT
//        val conf = new SparkConf().setMaster("local[*]").setAppName("FistDemo")
//        val sc = new SparkContext(conf)
//        val rdd = sc.parallelize(Array(5,10,30))
//        println(rdd.reduce(_+_))

//    LECTOR DE JSON
//    val bufferedSource = io.Source.fromFile("C:\\Users\\Consultant\\Downloads\\Votes.json")
//    val data = for (lines <- bufferedSource.getLines) yield {
//      Map("Id" -> ujson.read(lines)("Id").str, "PostId" -> ujson.read(lines)("PostId").str, "VoteTypeId" -> ujson.read(lines)("VoteTypeId").str)
//    }
//    data.foreach(println)


    //  CUENTA PALABRAS
    //    val sc = new miClasseScalass("local[*]", "SparkDemo01")
    //    val lines = read.textFile("C:\\Users\\Consultant\\Documents\\hadoop_options.TXT")
    //    val words = lines.flatMap(line => line.split(' '))
    //    val wordsKVRdd = words.map(x => (x, 1))
    //    val count = wordsKVRdd.reduceByKey((x, y) => x + y).map(x => (x._2, x._1)).sortByKey(false).map(x => (x._2, x._1)).take(10)
    //
    //    count.foreach(println)
  }
}