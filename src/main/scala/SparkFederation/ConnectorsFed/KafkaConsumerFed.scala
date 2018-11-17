package SparkFederation.ConnectorsFed


import java.util.Collections
import SparkFederation.Lib.KafkaProperties
import org.apache.kafka.clients.consumer.KafkaConsumer

class KafkaConsumerFed[U] (val groupId : String,
                        val typeCons: String,
                        val deserializer: String
                       ){

  def this (groupId: String, typeProd: String) {

    this (groupId
      ,typeProd
      ,"org.apache.kafka.common.serialization.StringDeserializer"
    )

  }

    val properties = KafkaProperties.createKafkaPropsCons(groupId, this.deserializer)
    // reference: https://github.com/smallnest/kafka-example-in-scala/blob/master/src/main/scala/com/colobu/kafka/ScalaConsumerExample.scala
    val consumer = configureConsumer(this.groupId , this.typeCons)

    def configureConsumer( groupId: String, typeCons: String) : KafkaConsumer[String,U] = {

      //val properties = new KafkaProperties(groupId, this.deserializer)
      //val kafConsumer = new KafkaConsumer[String,U](properties.KafkaPropsCons)
      val properties = KafkaProperties.createKafkaPropsCons(groupId, this.deserializer)
      val kafConsumer = new KafkaConsumer[String,U](properties)

      print ("$$$$ -> " + typeCons + " - " + KafkaProperties.getStandardTopic(typeCons))

      kafConsumer.subscribe(Collections.singletonList(KafkaProperties.getStandardTopic(typeCons)))
      kafConsumer
    }

    def shutdown(): Unit = {

      if (consumer != null) {
        consumer.close()
      }
    }

}
