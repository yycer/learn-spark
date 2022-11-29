import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 * @author Frankie
 * @since 2022-11-28 21:52 
 */
object StudyDataSource2 {
    def main(args: Array[String]): Unit = {
        val sparkSession = SparkSession.builder().config("spark.master", "local").getOrCreate()
        val csvFilePath = "/Users/yaoyichen/IdeaProjects/learn-spark/resources/person.csv"

        val schema: StructType = StructType(Array(
            StructField("name", StringType),
            StructField("age", IntegerType)
        ))

        val df : DataFrame = sparkSession.read.format("csv").schema(schema).option("header", value = true).load(csvFilePath)
        df.show
    }
}
