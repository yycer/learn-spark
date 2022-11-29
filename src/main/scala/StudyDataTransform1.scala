import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}

/**
 * @author Frankie
 * @since 2022-11-28 21:52 
 */
object StudyDataTransform1 {
    def main(args: Array[String]): Unit = {
        val sparkSession = SparkSession.builder().config("spark.master", "local").getOrCreate()

        val seq = Seq(("yyc", 26), ("Jack", 30))
        val rdd = sparkSession.sparkContext.parallelize(seq)
        val schema: StructType = StructType(Array(
            StructField("name", StringType),
            StructField("age", IntegerType)
        ))
        val rowRDD = rdd.map(field => Row(field._1, field._2))
        val df = sparkSession.createDataFrame(rowRDD, schema)

        df.createTempView("t1")
        val query = "select * from t1"
        val result = sparkSession.sql(query)
        println("Start to print")
        result.explain()
        println("Finish to print")
    }
}
