package SparkFederation.Lanzador

import org.apache.spark.sql.{Column, DataFrame, SparkSession}
import SparkFederation.ConnectorsFed.KafkaQueryResult
import SparkFederation.Lib.SparkProperties
import scala.collection.JavaConversions._
import SparkFederation.ClientFed.SimpleClientFed
import SparkFederation.ServerFed.SimpleServerFed


object Launcher extends App {

  override def main(args : Array[String]): Unit = {


    implicit val session: SparkSession = SparkProperties.ss

    val client = new SimpleClientFed( "StandarClient")
    val server = new SimpleServerFed("serverCluster")
    val query = "select b.JURISDICTION_NAME,a.ZIP_code,a.Complaint_ID from Consumer_Complaints a inner join Demographic_Statistics_By_Zip_Code b on a.ZIP_code = b.JURISDICTION_NAME"

    println(s"Server Ok: ${server.initializeServer}")
    println(s"Server ID: ${server.SERVER_ID}")
    println(s"Client ID: ${client.CLIENT_ID}")
    println(s"Client Topics: ${client.topicsClient.get(0)} -- ${client.topicsClient.get(1)}")



    // Este es el end2end


    client.summitQuery(query)

    // Servidor
    val message = server.listenQuery()

    println ("** antes de update Server")
    server.updateServer()
    println ("** Despues de update Server")
    val resultQuery: KafkaQueryResult = server.exeQueryFed(message)

    println ("** Resultado en el server")
    println(resultQuery)

    server.sendStatusResult(message.topicsId.get(0),resultQuery)

    if (resultQuery.typeQuery == "SelectStatement"){
      server.sendResult(message.topicsId.get(1),resultQuery.dataframe.get )
    } else {
        println ("** Se envia el resultado: ")
        server.syncCluster(resultQuery.query)
    }

    // Cliente
    println ("** Parte del cliente getStatusResult")
    val statusQuery = client.getStatusResult()
    var dfSelect = Option.empty[DataFrame]

    if (statusQuery.typeQuery == "SelectStatement"){
      val querySchema = statusQuery.schemaDataframe.get
      val columns: IndexedSeq[Column] = (0 to (querySchema.size -1)).map(client.getColAtIndex(_,querySchema))
    println ("** Parte del cliente getQueryResult")
      val selectResult = client.getQueryResult()
    println ("** despuesde ***+")
      dfSelect = Option[DataFrame](selectResult.select(columns: _*))


    }

    if (dfSelect.isDefined){
      dfSelect.get.show(false)
    }
  }

}

