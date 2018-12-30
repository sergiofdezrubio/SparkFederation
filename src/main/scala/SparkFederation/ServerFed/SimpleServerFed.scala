package SparkFederation.ServerFed

import SparkFederation.ConnectorsFed.{KafkaClientMessage, KafkaConsumerFed, KafkaProducerFed}
import SparkFederation.Lib.KafkaProperties
import org.apache.kafka.clients.consumer.{ConsumerRecord, ConsumerRecords}

import scala.collection.JavaConversions._

class SimpleServerFed (val idClient : String, val groupId : String ) {

  val serverSummiter = new KafkaProducerFed(
      this.idClient,KafkaProperties.getStandardTopic("server")
  )
  final val queryListener  = new KafkaConsumerFed[KafkaClientMessage](
      this.groupId
    ,"query"
    ,"SparkFederation.ConnectorsFed.KafkaClientDeserializer"
  )



  def listenQuery(): KafkaClientMessage ={

    val consumer = this.queryListener.consumer
    var result :  KafkaClientMessage = null
    var flag = 0

    while (flag == 0) {

      val records = consumer.poll(100)

      println("Antes bucle " + records.count() + " vacio: " + records.isEmpty())

      for (record <- records.iterator()) {
        println("Received message: (" + record.key() + ", " + record.value() + ") at offset " + record.offset())
        result = record.value()
        flag = 1
      }

    }
    consumer.commitSync()

    result
    // llamar a la clase que maneja el contexto de spark y lanza la query

  }


  def executeQuery(clientRecord: KafkaClientMessage ) : Unit = {


  }

}
