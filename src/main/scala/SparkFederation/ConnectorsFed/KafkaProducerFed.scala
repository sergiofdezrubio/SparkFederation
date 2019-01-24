package SparkFederation.ConnectorsFed

import java.util.Properties
import SparkFederation.Lib.KafkaProperties
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}

class KafkaProducerFed [U] (val idProducer: String,
                            val typeProd: String,
                            val serializer: String) {

  def this (groupId: String, typeProd: String) {

    this (groupId
          ,typeProd
          ,"org.apache.kafka.common.serialization.StringSerializer")

  }

  // reference: https://github.com/smallnest/kafka-example-in-scala/blob/master/src/main/scala/com/colobu/kafka/ScalaProducerExample.scala
  val producer = configureProducer(this.idProducer,this.typeProd)

  def configureProducer( idProducer: String, typeProd: String) : KafkaProducer [String, U] = {
    val topic = KafkaProperties.getStandardTopic(this.typeProd)

    //val properties  = new KafkaProperties(idClient,this.serializer)
    //val kafProducer = new KafkaProducer[String, U](properties.KafkaPropsProd)

    val properties  = KafkaProperties.createKafkaPropsProd(idProducer,this.serializer)
    val kafProducer = new KafkaProducer[String, U](properties)

    kafProducer
  }


  def sendMessage (data : U , topic: String): Unit = {
    producer.send(new ProducerRecord[String,U](topic, data)).get()


  }
}

