package cn.xpleaf.es.spark.write

import org.apache.spark.{SparkConf, SparkContext}
import org.elasticsearch.spark._

/**
  * @author xpleaf
  * @date 2019/1/7 11:10 PM
  *
  * 使用map将数据写入es
  */
object SampleEsSpark {

    def main(args: Array[String]): Unit = {
        // init spark
        val conf = new SparkConf()
            .setMaster("local[2]")
            .setAppName(s"${SampleEsSpark.getClass.getSimpleName}")
        conf.set("es.nodes", "localhost:9200")
            .set("es.resource", "es_spark_index/es_spark_type")
        val sc = new SparkContext(conf)

        // create map
        val numbers = Map("one" -> 1, "two" -> 2, "three" -> 3)
        val airports = Map("arrival" -> "Otopeni", "SFO" -> "San Fran")

        // write to es
        sc.makeRDD(Seq(numbers, airports)).saveToEs("spark/docs")

        sc.stop()
    }

}
