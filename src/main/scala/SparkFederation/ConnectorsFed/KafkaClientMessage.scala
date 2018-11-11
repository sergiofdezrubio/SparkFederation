package SparkFederation.ConnectorsFed

@SerialVersionUID(100L)
class KafkaClientMessage (val topicId: String,val query: String) extends Serializable {

  override def toString: String = "Message client: topicId -> " + this.topicId + " query -> " + this.query

}
