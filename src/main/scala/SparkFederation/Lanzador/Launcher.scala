package SparkFederation.Lanzador


import java.time.Duration
import java.util.{Collections, Properties}

import SparkFederation.ConnectorsFed.{KafkaClientMessage, KafkaConsumerFed, KafkaProducerFed}
import SparkFederation.Lib.KafkaProperties
import org.apache.kafka.clients.admin.{AdminClient, AdminClientConfig, ListTopicsOptions}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.clients.consumer.ConsumerRecords

import scala.collection.JavaConversions._
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, SubqueryAlias}
import java.util.Properties

import SparkFederation.ClientFed.SimpleClientFed
import SparkFederation.ServerFed.SimpleServerFed
import org.apache.kafka.clients.admin.{AdminClient, ListTopicsOptions, NewTopic}

import scala.collection.JavaConverters._
import org.apache.kafka.clients.admin.AdminClientConfig


// http://localhost:50070/

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
    /*
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
*/
    /*
    val KafkaServer = "localhost:9092"
    val topic = "default"
    val zookeeperConnect = KafkaServer
    val sessionTimeoutMs = 10 * 1000
    val connectionTimeoutMs = 8 * 1000

    val partitions = 1
    val replication:Short = 1
    val topicConfig = new Properties() // add per-topic configurations settings here

    val config = new Properties
    config.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, KafkaServer)
    val admin = AdminClient.create(config)

    admin.deleteTopics(List(topic).asJavaCollection)


    val existing1 = admin.listTopics(new ListTopicsOptions().timeoutMs(500).listInternal(true))
    val nms1 = existing1.names()
    //nms1.get().asScala.foreach(nm => println(nm)) // nms.get() fails


    val newTopic = new NewTopic(topic, partitions, replication)
    newTopic.configs(Map[String,String]().asJava)
    val ret = admin.createTopics(List(newTopic).asJavaCollection)
    //ret.all().get() // Also fails
    val a = List[String]()


    //println ("-- " + ret.values().keySet().iterator().next())
    println ("--1 " + ret.values().asScala.keys.head)


    val existing = admin.listTopics(new ListTopicsOptions().timeoutMs(500).listInternal(true))
    val nms = existing.names()
   // nms.get().asScala.foreach(nm => println(nm)) // nms.get() fails


    admin.close()
    */

    //KafkaProperties.deleteTopic("ClientTopic_1")
    //KafkaProperties.deleteTopic("SummitQuery")
    //KafkaProperties.createTopic("SummitQuery")
    val query = "select * from table_1 as a left join table_2 as b on a.id=b.id where a.id = 3"
    val client = new SimpleClientFed("client1","group1")
    val server = new SimpleServerFed("server","serverCluster")
    try {

      client.summitQuery(query)

      server.listenQuery()

      client.shutdown()
    } catch {
      case e : Exception => {
        client.shutdown()
        println (e.toString)
      }

    }
  }

}
