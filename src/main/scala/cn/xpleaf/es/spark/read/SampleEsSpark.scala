package cn.xpleaf.es.spark.read

import org.apache.spark.{SparkConf, SparkContext}
import org.elasticsearch.spark._

/**
  * @author xpleaf 
  * @date 2019/1/8 2:35 PM
  *
  * 从es读取数据
  * Note：
  * 1.可以直接读取为rdd，然后使用rdd.take(10)，可以取一定数量的数据大小，不需要尝试在query里面设置大小，不行
  * 这时，take(10)就不需要从es真的读取所有数据到spark的rdd分区中，然后再取10条，take 10的时候，才真正从es去读取10条，
  * 所以，要慎用rdd.count()，因为这时就会去全量读取es中的数据，这时量就非常很了
  * 2.sc.esRDD(resource: String, query: String)，后面是可以接查询语句的，当然也可以的conf中设置，conf.set("es.query", "")
  *
  * 所以查看一下es-hadoop的configuration文档还是有必要的，因为里面提供了很多配置说明
  * https://www.elastic.co/guide/en/elasticsearch/hadoop/5.6/configuration.html
  */
object SampleEsSpark {

    def main(args: Array[String]): Unit = {
        // init spark
        val conf = new SparkConf()
            .setMaster("local[2]")
            .setAppName(s"${SampleEsSpark.getClass.getSimpleName}")
            .set("es.nodes", "localhost:9200")

        val sc = new SparkContext(conf)

        // 从es读取数据转换为rdd
        val rdd = sc.esRDD("spark/docs")

        // 先缓存一下
        rdd.cache

        rdd.foreach(println)    // (1,Map(iata -> OTP, name -> Otopeni))

        rdd.foreach{
            case (id, dataMap) =>
                println("_id: " + id)
                println("doc: " + dataMap)
        }

        sc.stop()
    }

}
