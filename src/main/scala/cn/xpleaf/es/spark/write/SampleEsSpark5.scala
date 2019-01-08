package cn.xpleaf.es.spark.write

import org.apache.spark.{SparkConf, SparkContext}
import org.elasticsearch.spark._

/**
  * @author xpleaf 
  * @date 2019/1/8 11:45 AM
  *
  * 将额外的元数据信息，如指定文档的_id来写入es
  */
object SampleEsSpark5 {

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

        // init rdd
        val airportsRDD = sc.makeRDD(Seq((1, otp), (2, muc), (3, sfo)))

        airportsRDD.saveToEsWithMeta("spark/docs")

        sc.stop()
    }

}
