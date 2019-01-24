package SparkFederation.ConnectorsFed

@SerialVersionUID(100L)
class KafkaClientMessage (val topicsId: Seq[String], val query: String) extends Serializable {

  override def toString: String = "Message client: topicId -> " + this.topicsId.mkString + " query -> " + this.query

}
