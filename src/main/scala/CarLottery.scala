import org.apache.spark.sql.functions.{col, count, lit}
import org.apache.spark.sql.{SparkSession, functions}

/**
 * @author Frankie
 * @since 2022-11-04 20:54 
 */
object CarLottery {

    val apply_path = "/Users/yaoyichen/Downloads/car_lottery_data/apply"
    val lucky_path = "/Users/yaoyichen/Downloads/car_lottery_data/lucky"

    def main(args: Array[String]) = {
        val sparkSession = SparkSession.builder().config("spark.master", "local").getOrCreate()
        val applyNumbersDF = sparkSession.read.parquet(apply_path)
        val luckyDogsDF = sparkSession.read.parquet(lucky_path)

        val filteredLuckyDogs = luckyDogsDF.filter(col("batchNum") >= "201601").select("carNum")
        val jointDF = applyNumbersDF.join(filteredLuckyDogs, Seq("carNum"), "inner")
        val multipliers = jointDF.groupBy(col("batchNum"), col("carNum"))
            .agg(count(lit(1)).alias("multiplier"))
        val uniqueMultipliers = multipliers.groupBy("carNum")
            .agg(functions.max("multiplier").alias("multiplier"))
        val result = uniqueMultipliers.groupBy("multiplier")
            .agg(count(lit(1)).alias("cnt")).orderBy("multiplier")

        result.collect()
    }
}
