package SparkFederation.ConnectorsFed

import java.io.{ByteArrayInputStream, ObjectInputStream}
import java.util

import org.apache.kafka.common.serialization.Deserializer

class KafkaQueryResultDeserializer extends Deserializer[KafkaQueryResult]{

  override def configure(configs: util.Map[String,_],isKey: Boolean):Unit = {

  }
  override def deserialize(topic:String,bytes: Array[Byte]) = {
    val byteIn = new ByteArrayInputStream(bytes)
    val objIn = new ObjectInputStream(byteIn)
    val obj = objIn.readObject().asInstanceOf[KafkaQueryResult]
    byteIn.close()
    objIn.close()
    obj
  }
  override def close():Unit = {

  }

}
