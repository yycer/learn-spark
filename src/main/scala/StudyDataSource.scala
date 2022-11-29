import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

/**
 * @author Frankie
 * @since 2022-11-28 21:52 
 */
object StudyDataSource {
    def main(args: Array[String]): Unit = {
        val sparkSession = SparkSession.builder().config("spark.master", "local").getOrCreate()
        val seq: Seq[(String, Int)] = Seq(("yicyao", 26), ("Jack", 30))
        val rdd: RDD[(String, Int)] = sparkSession.sparkContext.parallelize(seq)

        // 1. 从 Driver 创建 DataFrame
        val schema: StructType = StructType(Array(
            StructField("name", StringType),
            StructField("age", IntegerType)
        ))
        val rowRDD: RDD[Row] = rdd.map(field => Row(field._1, field._2))
        val dataFrame: DataFrame = sparkSession.createDataFrame(rowRDD, schema)
        dataFrame.show

    }
}
