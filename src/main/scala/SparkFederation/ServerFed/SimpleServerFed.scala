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
      this.groupId,
    //KafkaProperties.getStandardTopic("query")
    "query"
      ,"SparkFederation.ConnectorsFed.KafkaClientDeserializer"
  )



  def listenQuery(): KafkaClientMessage ={

    val consumer = this.queryListener.consumer
    var resoult :  KafkaClientMessage = null

    var flag = 0

    println (" --> " + this.queryListener.properties)




    while (flag == 0) {

      val records = consumer.poll(100)

      println("Antes bucle " + records.count() + " vacio: " + records.isEmpty())

      for (record <- records.iterator()) {
        println("Received message: (" + record.key() + ", " + record.value() + ") at offset " + record.offset())
        resoult = record.value()
        flag = 1
      }

    }
    consumer.commitSync()

    resoult
    // llamar a la clase que maneja el contexto de spark y lanza la query

  }


  def executeQuery(clientRecord: KafkaClientMessage ) : Unit = {


  }

}
