package cn.xpleaf.es.spark.write

import org.apache.spark.{SparkConf, SparkContext}

import org.elasticsearch.spark._
// Import the Metadata enum
import org.elasticsearch.spark.rdd.Metadata._

/**
  * @author xpleaf 
  * @date 2019/1/8 11:52 AM
  *
  * 将更丰富的元数据信息写入es
  */
object SampleEsSpark6 {

    def main(args: Array[String]): Unit = {
        // init spark
        val conf = new SparkConf()
            .setMaster("local[2]")
            .setAppName(s"${SampleEsSpark.getClass.getSimpleName}")
        conf.set("es.nodes", "localhost:9200")
            .set("es.resource", "es_spark_index/es_spark_type")
        val sc = new SparkContext(conf)

        // init data
        val otp = Map("iata" -> "OTP", "name" -> "Otopeni")
        val muc = Map("iata" -> "MUC", "name" -> "Munich")
        val sfo = Map("iata" -> "SFO", "name" -> "San Fran")

        // init metadata
        // metadata for each document
        // note it's not required for them to have the same structure
        // Important: Metadata enum has been imported above
        val otpMeta = Map(ID -> 1, TTL -> "3h")
        val mucMeta = Map(ID -> 2, VERSION -> "23")
        val sfoMeta = Map(ID -> 3)

        // init rdd
        val airportsRDD = sc.makeRDD(Seq((otpMeta, otp), (mucMeta, muc), (sfoMeta, sfo)))

        airportsRDD.saveToEsWithMeta("spark/docs")

        sc.stop()
    }

}
