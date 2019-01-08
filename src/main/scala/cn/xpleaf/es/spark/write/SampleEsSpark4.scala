package cn.xpleaf.es.spark.write

import org.apache.spark.{SparkConf, SparkContext}
import org.elasticsearch.spark.rdd.EsSpark

/**
  * @author xpleaf 
  * @date 2019/1/8 11:29 AM
  *
  * 将json数据写入es
  */
object SampleEsSpark4 {

    def main(args: Array[String]): Unit = {
        // init spark
        val conf = new SparkConf()
            .setMaster("local[2]")
            .setAppName(s"${SampleEsSpark.getClass.getSimpleName}")
        conf.set("es.nodes", "localhost:9200")
            .set("es.resource", "es_spark_index/es_spark_type")
        val sc = new SparkContext(conf)

        // create json data
        val json1 = """{"reason" : "business", "airport" : "SFO"}"""
        val json2 = """{"participants" : 5, "airport" : "OTP"}"""

        val rdd = sc.parallelize(Seq(json1, json2))

        EsSpark.saveJsonToEs(rdd, "spark/docs")

        sc.stop()
    }
}
