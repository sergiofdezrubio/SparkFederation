package SparkFederation.Lib

import java.util.Properties

import kafka.admin.AdminUtils
import kafka.utils.ZkUtils
import org.I0Itec.zkclient.ZkClient
import kafka.zk.AdminZkClient
import kafka.utils.ZKStringSerializer
import org.I0Itec.zkclient.serialize.ZkSerializer
import org.apache.kafka.clients.admin.CreateTopicsResult
import org.apache.zookeeper.ZKUtil


import org.apache.kafka.clients.admin.AdminClientConfig
import org.apache.kafka.clients.admin.{AdminClient, ListTopicsOptions, NewTopic}
import scala.collection.JavaConverters._

// AdminZkClient

object KafkaCreateTopic {

  val zookeeperConnect = "localhost:2181"
  val sessionTimeoutMs: Int = 10 * 1000
  val connectionTimeoutMs: Int = 8 * 1000

  val zkClient = new ZkClient(
    zookeeperConnect,
    sessionTimeoutMs,
    connectionTimeoutMs,ZKStringSerializer$/MODULE$
  )

  val prueba = ZkUtils.createZkClientAndConnection()
/*



  val hola = zkClient.setZkSerializer(ZKStringSerializer.MODULE$ )

  def CreateKafkaTopic(topic: String, zookeeperHosts: String, partitionSize: Int, replicationCount: Int, connectionTimeoutMs: Int = 10000, sessionTimeoutMs: Int = 10000): Boolean = {
    if (List(zookeeperHosts).contains(topic) ) {
      return false
    }
    val zkUtils = ZkUtils.apply(zookeeperHosts, sessionTimeoutMs, connectionTimeoutMs, false)
    AdminUtils.createTopic( zkUtils, topic, partitionSize, replicationCount, new Properties())
    zkUtils.close()
    true
  }
*/
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