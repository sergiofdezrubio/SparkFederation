package SparkFederation.Lib

import java.util.Properties

import org.apache.kafka.clients.consumer.ConsumerConfig
import scala.collection.JavaConversions._

object KafkaProperties {

  private val brokers =  "localhost:9092"
  private val QueryTopic : String = "SummitQuery"
  private val ServerTopic : String = "ServerTopic"

  def getTopic(typeProCons: String ): String = {

    var result = "default"

    if (typeProCons == "query") {
      result=this.QueryTopic
    }
    if (typeProCons == "server") {
      result=this.ServerTopic
    }

    result
  }

}

class KafkaProperties (val groupId : String,val serializer: String) {

/*
  def this (groupId: String) {

    this (groupId,
          "org.apache.kafka.common.serialization.StringSerializer",
          "org.apache.kafka.common.serialization.StringDeserializer"
    )

  }
*/
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