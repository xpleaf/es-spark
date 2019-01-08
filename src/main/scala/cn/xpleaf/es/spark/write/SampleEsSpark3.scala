package cn.xpleaf.es.spark.write

import org.apache.spark.{SparkConf, SparkContext}
import org.elasticsearch.spark.rdd.EsSpark

/**
  * @author xpleaf
  * @date 2019/1/8
  *
  * 将文档id作为es docId将数据写入es
  */
object SampleEsSpark3 {

    def main(args: Array[String]): Unit = {
        // init spark
        val conf = new SparkConf()
            .setMaster("local[2]")
            .setAppName(s"${SampleEsSpark.getClass.getSimpleName}")
        conf.set("es.nodes", "localhost:9200")
            .set("es.resource", "es_spark_index/es_spark_type")
        val sc = new SparkContext(conf)

        case class Blog(id: String, title: String, content: String)

        val blog1 = Blog("1", "do you like es?", "I love es!!!")
        val blog2 = Blog("2", "do you like spark?", "I love spark!!!")

        // write to es，指定es内部的docId为我们doc中的id字段
        val rdd = sc.parallelize(Seq(blog1, blog2))
        EsSpark.saveToEs(rdd, "spark/docs", Map("es.mapping.id" -> "id"))

        sc.stop()
    }

}
