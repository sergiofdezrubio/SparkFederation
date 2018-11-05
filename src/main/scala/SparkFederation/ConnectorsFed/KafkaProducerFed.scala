package SparkFederation.ConnectorsFed

import SparkFederation.Lib.KafkaProperties
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}

class KafkaProducerFed {
  val producer = new KafkaProducer[String,String](KafkaProperties.ClientProps)



  def summitQuery (query : String ): Unit = {

    val data = new ProducerRecord[String, String](KafkaProperties.ClientTopic,query)
    producer.send(data).get()

  }
}

