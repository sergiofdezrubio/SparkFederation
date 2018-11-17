package SparkFederation.Lib


import org.apache.kafka.clients.consumer.ConsumerConfig
import java.util.Properties
import org.apache.kafka.clients.admin.{AdminClient, ListTopicsOptions, NewTopic}
import scala.collection.JavaConverters._
import org.apache.kafka.clients.admin.AdminClientConfig

object KafkaProperties {

  private val brokers =  "localhost:9092"
  private val QueryTopic : String = "SummitQuery"
  private val ServerTopic : String = "ServerTopic"

  private val KafkaServer = brokers
  private val admin = createAdmin()
  private var clientConected = 0

  def getStandardTopic(typeProCons: String ): String = {

    var result = "default"

    if (typeProCons == "query") {
      result=this.QueryTopic
    }
    if (typeProCons == "server") {
      result=this.ServerTopic
    }
    result
  }

  def createClientTopicId (): String = {

    this.clientConected += 1

    val topicId = "ClientTopic_" + this.clientConected
    topicId
  }


  // https://stackoverflow.com/questions/47871708/kafka-1-0-0-admin-client-cannot-create-topic-with-eofexception

  def createAdmin(): AdminClient = {
    val config = new Properties
    config.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, KafkaServer)
    val admin = AdminClient.create(config)
    admin
  }

  def createTopic(topic :String, partition: Short = 1, replication:Short = 1 ) : String = {

    val newTopic = new NewTopic(topic, partition, replication)
    newTopic.configs(Map[String, String]().asJava)
    val ret = admin.createTopics(List(newTopic).asJavaCollection)
    //ret.all().get() <-- Contains a Map with the result of "createTopics"
    // as it only create one topic, this function we only return one string
    //val result = ret.values().keySet().iterator().next()
    val result = ret.values().asScala.keys.head
    result
  }

  def listTopics(): Iterable[String] = {
    val existing = admin.listTopics(new ListTopicsOptions().timeoutMs(500).listInternal(true))
    val nms = existing.names()
    nms.get().asScala
    // nms.get().asScala contains an iterator with all existing topics
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

  def shutDownAdmin (): Unit = {

    admin.close()

  }

  def createKafkaPropsProd(groupId : String, serializer: String): Properties = {
    val KafkaPropsProd = new  Properties()
    KafkaPropsProd.put("bootstrap.servers", KafkaProperties.brokers)
    KafkaPropsProd.put("client.id", groupId)
    KafkaPropsProd.put("key.serializer", serializer)
    KafkaPropsProd.put("value.serializer", serializer)

    KafkaPropsProd
  }

  def createKafkaPropsCons(groupId : String, deserializer: String = "org.apache.kafka.common.serialization.StringDeserializer"): Properties = {
    val KafkaPropsCons = new Properties()
    KafkaPropsCons.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KafkaProperties.brokers)
    KafkaPropsCons.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true")
    KafkaPropsCons.put(ConsumerConfig.GROUP_ID_CONFIG, groupId)
    KafkaPropsCons.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "1000")
    KafkaPropsCons.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, "30000")
    KafkaPropsCons.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, deserializer)
    KafkaPropsCons.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, deserializer)
    KafkaPropsCons.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,"latest")
    KafkaPropsCons
  }
}

/*
class KafkaProperties (val groupId : String,val serializer: String) {

  // Topic ClientProducer: "SummitQuery"
  //  kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic SummitQuery
  //  kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic SummitQuery --from-beginning
  // Topic ServerProducer: "resultQuery"
  //  kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic ResultQuery


  val KafkaPropsProd = new  Properties()
  KafkaPropsProd.put("bootstrap.servers", KafkaProperties.brokers)
  KafkaPropsProd.put("client.id", this.groupId)
  KafkaPropsProd.put("key.serializer", this.serializer)
  KafkaPropsProd.put("value.serializer", this.serializer)

  val KafkaPropsCons = new Properties()
  KafkaPropsCons.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KafkaProperties.brokers)
  KafkaPropsCons.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true")
  KafkaPropsCons.put(ConsumerConfig.GROUP_ID_CONFIG, groupId)
  KafkaPropsCons.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "1000")
  KafkaPropsCons.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, "30000")
  KafkaPropsCons.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, this.serializer)
  KafkaPropsCons.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, this.serializer)


}
*/