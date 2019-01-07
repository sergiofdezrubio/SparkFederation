package SparkFederation.ConnectorsFed

import java.io.{ByteArrayOutputStream, ObjectOutputStream}
import java.util

import org.apache.kafka.common.serialization.Serializer

class KafkaQueryResultSerializer extends Serializer[KafkaQueryResult]{

  override def configure(configs: util.Map[String,_],isKey: Boolean):Unit = {

  }

  override def serialize (topic:String, data: KafkaQueryResult ):Array[Byte] = {

    try {

      val byteOut = new ByteArrayOutputStream()
      val objOut = new ObjectOutputStream(byteOut)
      objOut.writeObject(data)
      objOut.close()
      byteOut.close()
      byteOut.toByteArray

    }
    catch {

      case ex:Exception => {
        println ("************************* fallo aqui ")
        throw new Exception(ex.getMessage)
      }
    }
  }

  override def close():Unit = {

  }

}
