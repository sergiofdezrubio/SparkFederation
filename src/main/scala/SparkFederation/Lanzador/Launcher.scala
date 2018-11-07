package SparkFederation.Lanzador

import java.time.Duration
import java.util.Collections

import SparkFederation.ConnectorsFed.{KafkaConsumerFed, KafkaProducerFed}
import SparkFederation.Lib.KafkaProperties
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.clients.consumer.ConsumerRecords
import scala.collection.JavaConversions._

object Launcher {

  def main(args : Array[String]): Unit = {
    //val exProd = new KafkaProducerFed(KafkaProperties.KafkaPropsProd,KafkaProperties.QueryTopic)
    val exProd = new KafkaProducerFed("ClientQuery_1","query")
    val exCons = new KafkaConsumerFed("severGroup_1","query")
    //val exCons = new KafkaConsumerFed(KafkaProperties.KafkaPropsCons,KafkaProperties.QueryTopic)
    println("antes de enviar mensaje")
    exProd.sendMessage("esto es una prueba hijo puta cabroncete 5")

    println("despues de enviar mensaje")
    val exConsumer = exCons.consumer;
    println("antes de consumir")

    var flag = 0
    while (flag == 0) {
      val records = exConsumer.poll(100)

      println("Antes bucle " + records.count() + " vacio: " + records.isEmpty())

      for (record <- records.iterator()) {
        println("entro")
        println("Received message: (" + record.key() + ", " + record.value() + ") at offset " + record.offset())
        flag = 1
      }
      flag = 1
    }
  }

}
