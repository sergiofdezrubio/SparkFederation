package SparkFederation.ConnectorsFed

import java.util.Properties

import SparkFederation.Lib.KafkaProperties
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}

class KafkaProducerFed (val idClient: String, val typeProd: String ) {

  // reference: https://github.com/smallnest/kafka-example-in-scala/blob/master/src/main/scala/com/colobu/kafka/ScalaProducerExample.scala
  val producer = configureProducer(this.idClient,this.typeProd)
  val topic = KafkaProperties.getTopic(this.typeProd)

  def configureProducer( idClient: String, typeProd: String) : KafkaProducer[String,String] = {

    val properties = new KafkaProperties(idClient)
    val kafProducer = new KafkaProducer[String,String](properties.KafkaPropsProd)

    kafProducer
  }


  def sendMessage (data : String): Unit = {
    producer.send(new ProducerRecord[String, String](this.topic,data)).get()

  }
}

