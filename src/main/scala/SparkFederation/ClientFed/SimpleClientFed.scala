package SparkFederation.ClientFed

import SparkFederation.ConnectorsFed.{KafkaClientMessage, KafkaConsumerFed, KafkaProducerFed, KafkaQueryResult}
import SparkFederation.Lanzador.Launcher.getColAtIndex1
import SparkFederation.Lib.KafkaProperties
import org.apache.spark.sql.{Column, DataFrame, SparkSession}

import scala.collection.JavaConversions._

class SimpleClientFed (val groupId : String = "StandarClient") (implicit  ss : SparkSession) {

  final val CLIENT_ID = KafkaProperties.createUniqueId()
  val querySummiter  = new KafkaProducerFed[KafkaClientMessage](CLIENT_ID,KafkaProperties.getStandardTopic("query"),"SparkFederation.ConnectorsFed.KafkaClientSerializer")
  val serverListener = new KafkaConsumerFed[String](this.groupId,KafkaProperties.getStandardTopic("server"))
  val topicsClient    = createClientTopics(CLIENT_ID)
  val clientListener = new KafkaConsumerFed[KafkaQueryResult](this.groupId,this.topicsClient.get(0),"SparkFederation.ConnectorsFed.KafkaQueryResultDeserializer")

  def createClientTopics(clientId : String) : Seq[String] = Seq(s"${groupId}_${clientId}",s"${groupId}_${clientId}-RESULT" ).map( KafkaProperties.createTopic(_))

  def getColAtIndex(id:Int,schema: Array[String]): org.apache.spark.sql.Column = org.apache.spark.sql.functions.col(s"valueArray")(id).as(schema(id))

  def clientMainQueryExecution (query : String) : Unit = {
    summitQuery(query)
    val queryStatusResult = getStatusResult()
    var dfSelect = Option.empty[DataFrame]


    if (queryStatusResult.typeQuery == "SelectStatement"){
      val querySchema = queryStatusResult.schemaDataframe.get
      val columns: IndexedSeq[Column] = (0 to (querySchema.size -1)).map(getColAtIndex(_,querySchema))
      val selectResult = getQueryResult()
      dfSelect = Option[DataFrame](selectResult.select(columns: _*))
    }

    if (dfSelect.isDefined){
      dfSelect.get.show(false)
    }
  }

  def getQueryResult() : DataFrame = {

    val df = ss
      .read
      .format("kafka")
      .option("kafka.bootstrap.servers", KafkaProperties.brokers)
      .option("subscribe", topicsClient.get(1))
      .load()

    val dfTemp = df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")

    val dfTemp1 = dfTemp.withColumn("newValue", org.apache.spark.sql.functions.expr("substring(value, 2, length(value)-2)"))
    dfTemp1.withColumn("valueArray", org.apache.spark.sql.functions.split(dfTemp1("newValue"), ",").cast("array<String>"))

  }


  def summitQuery (query: String): Unit = {

    // create message to sent
    val message = new KafkaClientMessage(this.topicsClient,query)
    // sent query by querySummiter
    this.querySummiter.sendMessage(message, KafkaProperties.getStandardTopic( "query"))
    // listen to the new topic

  }

  def getStatusResult(): KafkaQueryResult ={



    val consumer = clientListener.consumer
    var result :  KafkaQueryResult = null
    var flag = 0


    while (flag == 0) {

      val records = consumer.poll(100)

      for (record <- records.iterator()) {
        result = record.value()
        flag = 1
      }

    }
    consumer.commitSync()

    result

  }

  def shutdown (): Unit ={
    topicsClient.map(KafkaProperties.deleteTopic)
  }

}
