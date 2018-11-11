package SparkFederation.Lib

import java.util.Properties

import org.apache.kafka.clients.admin.{AdminClient, ListTopicsOptions, NewTopic}

import scala.collection.JavaConverters._
import org.apache.kafka.clients.admin.AdminClientConfig

object KafkaCreateTopic {

  val KafkaServer = "localhost:9092"
  val admin = createAdmin()

  // https://stackoverflow.com/questions/47871708/kafka-1-0-0-admin-client-cannot-create-topic-with-eofexception

  def createAdmin(): AdminClient = {
    val config = new Properties
    config.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, KafkaServer)
    val admin = AdminClient.create(config)
    admin
  }

  def createTopic(topic :String, partition: Short = 1, replication:Short = 1 ) : Int = {

    var result = 0

    val topicsActives = listTopics()

    topicsActives.foreach(
      t => {
          if (t.equals(topic)) {
            result=1
          }

      }
    )
    if (result == 0) {
      val newTopic = new NewTopic(topic, partition, replication)
      newTopic.configs(Map[String, String]().asJava)
      val ret = admin.createTopics(List(newTopic).asJavaCollection)
      ret.all().get() // Also fails
    }

    result
  }

  def listTopics(): Iterable[String] = {
    val existing = admin.listTopics(new ListTopicsOptions().timeoutMs(500).listInternal(true))
    val nms = existing.names()
    nms.get().asScala
    // nms.get().asScala.foreach(nm => println(nm)) <- To print topics name
  }

  def deleteTopic (topic: String) : Int = {
    var result = 1
    val topicsActives = listTopics()

    topicsActives.foreach(
      t => {
        if (t.equals(topic)) {
          admin.deleteTopics(List(topic).asJavaCollection)
          result=0
        }

      }
    )
    result
  }

  def shutDown (): Unit ={

    admin.close()
  }
}
/*
mirar bien
import org.apache.kafka.clients.admin.{AdminClient, ListTopicsOptions, NewTopic}
import scala.collection.JavaConverters._

val zkServer = "localhost:2181"
val topic = "test1"

val zookeeperConnect = zkServer
val sessionTimeoutMs = 10 * 1000
val connectionTimeoutMs = 8 * 1000


val partitions = 1
val replication:Short = 1
val topicConfig = new Properties() // add per-topic configurations settings here

import org.apache.kafka.clients.admin.AdminClientConfig
val config = new Properties
config.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, zkServer)
val admin = AdminClient.create(config)

val existing = admin.listTopics(new ListTopicsOptions().timeoutMs(500).listInternal(true))
val nms = existing.names()
nms.get().asScala.foreach(nm => println(nm)) // nms.get() fails

val newTopic = new NewTopic(topic, partitions, replication)
newTopic.configs(Map[String,String]().asJava)
val ret = admin.createTopics(List(newTopic).asJavaCollection)
ret.all().get() // Also fails
admin.close()

*/