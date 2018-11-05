package SparkFederation.Lanzador

import SparkFederation.ConnectorsFed.KafkaProducerFed

object Launcher {

  def main(args : Array[String]): Unit = {
    val example = new KafkaProducerFed()
    example.summitQuery("esto es una prueba hijo puta cabroncete")
  }

}
