package SparkFederation.Lanzador


import java.time.Duration
import java.util.Collections

import SparkFederation.ConnectorsFed.{KafkaClientMessage, KafkaConsumerFed, KafkaProducerFed}
import SparkFederation.Lib.KafkaProperties
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.clients.consumer.ConsumerRecords

import scala.collection.JavaConversions._
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, SubqueryAlias}

object Launcher {

  def getTables(query: String): Seq[String] = {

    val sparkSession = SparkSession.builder
      .master("local")
      .appName("my-spark-app")
      .config("spark.driver.host","localhost")
      .master("local[*]")
      .getOrCreate()
    val logicalPlan = sparkSession.sessionState.sqlParser.parsePlan(query)
    import org.apache.spark.sql.catalyst.analysis.UnresolvedRelation
    logicalPlan.collect { case r: UnresolvedRelation => r.tableName  }


  }

  def getQueryPlan(query: String): Seq[(String,LogicalPlan)] = {

    val sparkSession = SparkSession.builder
      .master("local")
      .appName("my-spark-app")
      .config("spark.driver.host","localhost")
      .master("local[*]")
      .getOrCreate()
    val logicalPlan = sparkSession.sessionState.sqlParser.parsePlan(query)
    println (logicalPlan)
    logicalPlan.collect{ case r : SubqueryAlias => (r.alias,r.child)  }
  }

  def main(args : Array[String]): Unit = {

    val query = "select * from table_1 as a left join table_2 as b on a.id=b.id where a.id = 2"
    /*
    Launcher.getTables(query).foreach(println)

    val plan =Launcher.getQueryPlan(query)

    plan.foreach( tupla  => { println ( tupla._1 + " -- " + tupla._2) })
    */

    val exProd = new KafkaProducerFed[KafkaClientMessage](  "ClientQuery_1","query",
                                       "SparkFederation.ConnectorsFed.KafkaClientSerializer")
    val exCons = new KafkaConsumerFed[KafkaClientMessage]("severGroup_1","query",
                                      "SparkFederation.ConnectorsFed.KafkaClientDeserializer")

    val data = new KafkaClientMessage("topic_1", query)
    println("antes de enviar mensaje")

      exProd.sendMessage(data)



    println("despues de enviar mensaje")
    val exConsumer = exCons.consumer
    println("antes de consumir")

    var flag = 0
    while (flag == 0) {

      val records = exConsumer.poll(100)

      println("Antes bucle " + records.count() + " vacio: " + records.isEmpty())

      for (record <- records.iterator()) {
        println("entro")
        println("Received message: (" + record.key() + ", " + record.value() + ") at offset " + record.offset())
        flag = 1
      }
    }

  }

}
