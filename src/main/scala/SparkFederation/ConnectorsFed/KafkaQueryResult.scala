package SparkFederation.ConnectorsFed

import org.apache.spark.sql.DataFrame


@SerialVersionUID(100L)
class KafkaQueryResult(
                     val dataframe : Option[DataFrame]
                    ,val query : String
                    ,val typeQuery : String
                    ,val status : String
                    ,val  error : Option[Exception]
                    ,val schemaDataframe : Option[Array[String]]
                  )extends Serializable {

override def toString(): String = {
    if (dataframe.isDefined && error.isDefined && schemaDataframe.isEmpty) {
      val toPrint = s"Query Details:\n\tType Query:\t${typeQuery}\n\tStatement:\t${query}\n\tEstatus:\t${status}\n\t+" +
        s"Query Result (20 rows):\t${dataframe.get.show(20)}\n\t" +
        s"Error:\n\t\t${error.get}\n\t" +
        s"Schema:\tNone\n\t"
      toPrint
    } else if (dataframe.isDefined && error.isEmpty && schemaDataframe.isEmpty) {
      val toPrint = s"Query Details:\n\tType Query:\t${typeQuery}\n\tStatement:\t${query}\n\tEstatus:\t${status}\n\t+" +
        s"Query Result:\t${dataframe.get}\n\t" +
        s"Error:\tNone \n\t" +
        s"Schema:\tNone\n\t"
      toPrint

    } else if (dataframe.isDefined && error.isDefined && schemaDataframe.isDefined) {
      val toPrint = s"Query Details:\n\tType Query:\t${typeQuery}\n\tStatement:\t${query}\n\tEstatus:\t${status}\n\t+" +
        s"Query Result:\t${dataframe.get}\n\t" +
        s"Error:\n\t\t${error.get}\n\t" +
        s"Schema:\t${schemaDataframe.get.mkString(",")}\n\t"
      toPrint
    }else if (dataframe.isDefined && error.isEmpty && schemaDataframe.isDefined){
      val toPrint = s"Query Details:\n\tType Query:\t${typeQuery}\n\tStatement:\t${query}\n\tEstatus:\t${status}\n\t+" +
        s"Query Result:\t${dataframe.get}\n\t" +
        s"Error:\tNone \n\t" +
        s"Schema:\t${schemaDataframe.get.mkString(",")}\n\t"
      toPrint
    }else {
      val toPrint = s"Query Details:\n\tType Query:\t${typeQuery}\n\tStatement:\t${query}\n\tEstatus:\t${status}\n\t+" +
        s"Query Result (20 rows):\t None\n\t" +
        s"Error:\tNone\n\tSchema:\tNone"

      toPrint
    }
  }

}
