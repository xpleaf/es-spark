package cn.xpleaf.es.spark.write

import org.apache.spark.{SparkConf, SparkContext}
import org.elasticsearch.spark.rdd.EsSpark

/**
  * @author xpleaf
  * @date 2019/1/8
  *
  * 使用样例类将数据写入es
  */
object SampleEsSpark2 {

    def main(args: Array[String]): Unit = {
        // init spark
        val conf = new SparkConf()
            .setMaster("local[2]")
            .setAppName(s"${SampleEsSpark.getClass.getSimpleName}")
        conf.set("es.nodes", "localhost:9200")
            .set("es.resource", "es_spark_index/es_spark_type")
        val sc = new SparkContext(conf)

        // case class
        case class Trip(departure: String, arrival: String)

        // create case class obj，相比map就好很多，因为这时key或者field就不用指定了，直接使用case class中的参数名
        val upcomingTrip = Trip("OTP", "SFO")
        val lastWeekTrip = Trip("MUC", "OTP")

        // write to es
        // sc.makeRDD(Seq(upcomingTrip, lastWeekTrip)).saveToEs("spark/docs")
         val rdd = sc.parallelize(Seq(upcomingTrip, lastWeekTrip))
        EsSpark.saveToEs(rdd, "spark/docs")

        sc.stop()
    }

}
