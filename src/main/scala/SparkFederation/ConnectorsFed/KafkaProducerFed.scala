package SparkFederation.ConnectorsFed

import java.util.Properties
import SparkFederation.Lib.KafkaProperties
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}

class KafkaProducerFed [U] (val idClient: String,
                            val typeProd: String,
                            val serializer: String) {

  def this (groupId: String, typeProd: String) {

    this (groupId
          ,typeProd
          ,"org.apache.kafka.common.serialization.StringSerializer")

  }

  // reference: https://github.com/smallnest/kafka-example-in-scala/blob/master/src/main/scala/com/colobu/kafka/ScalaProducerExample.scala
  val producer = configureProducer(this.idClient,this.typeProd)
  val topic = KafkaProperties.getTopic(this.typeProd)

  def configureProducer( idClient: String, typeProd: String) : KafkaProducer [String, U] = {

    val properties  = new KafkaProperties(idClient,this.serializer)
    val kafProducer = new KafkaProducer[String, U](properties.KafkaPropsProd)

    kafProducer
  }


  def sendMessage (data : U): Unit = {
    println ("** " + data.toString)
    producer.send(new ProducerRecord[String,U](this.topic, data)).get()

  }
}

