package cn.xpleaf.es.spark.read

import org.apache.spark.{SparkConf, SparkContext}
import org.elasticsearch.spark._

/**
  * @author xpleaf 
  * @date 2019/1/8 3:44 PM
  *
  * 从es读取数据，data部分以json格式返回
  */
object SampleEsSpark2 {

    def main(args: Array[String]): Unit = {
        // init spark
        val conf = new SparkConf()
            .setMaster("local[2]")
            .setAppName(s"${SampleEsSpark.getClass.getSimpleName}")
            .set("es.nodes", "localhost:9200")

        val sc = new SparkContext(conf)

        // 从es读取数据转换为jsonRDD
        val jsonRDD = sc.esJsonRDD("spark/docs")

        jsonRDD.cache

        jsonRDD.foreach(println)    // (2,{"iata":"MUC","name":"Munich"})

        jsonRDD.foreach{
            case (id, jsonData) =>
                println("_id: " + id)           // _id: 1
                println("doc: " + jsonData)     // doc: {"iata":"OTP","name":"Otopeni"}
        }

        sc.stop()
    }

}
