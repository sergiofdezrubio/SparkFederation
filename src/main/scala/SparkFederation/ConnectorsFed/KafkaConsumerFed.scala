package SparkFederation.ConnectorsFed


import java.util.Collections
import SparkFederation.Lib.KafkaProperties
import org.apache.kafka.clients.consumer.KafkaConsumer

class KafkaConsumerFed (val groupId : String, val typeCons: String){
    // reference: https://github.com/smallnest/kafka-example-in-scala/blob/master/src/main/scala/com/colobu/kafka/ScalaConsumerExample.scala
    val consumer = configureConsumer(this.groupId , this.typeCons)

    def configureConsumer( groupId: String, typeCons: String) : KafkaConsumer[String,String] = {

      val properties = new KafkaProperties(groupId)
      val kafConsumer = new KafkaConsumer[String,String](properties.KafkaPropsCons)
      kafConsumer.subscribe(Collections.singletonList(KafkaProperties.getTopic(typeCons)))
      kafConsumer
    }

    def shutdown(): Unit = {

      if (consumer != null) {
        consumer.close()
      }
    }

}
