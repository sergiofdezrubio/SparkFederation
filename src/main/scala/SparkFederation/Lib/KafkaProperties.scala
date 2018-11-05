package SparkFederation.Lib

import java.util.Properties

object KafkaProperties {

  // Topic ClientProducer: "SummitQuery"
  //  kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic SummitQuery
  //  kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic SummitQuery --from-beginning
  // Topic ServerProducer: "resultQuery"
  //  kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic ResultQuery

  val ClientProps = new  Properties()
      ClientProps.put("bootstrap.servers", "localhost:9092")
      ClientProps.put("client.id", "ClientProducer")
      ClientProps.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
      ClientProps.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

  val ClientTopic = "SummitQuery"

}
