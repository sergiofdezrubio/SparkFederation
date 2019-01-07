package SparkFederation.ConnectorsFed

import org.apache.spark.sql.DataFrame


@SerialVersionUID(100L)
class KafkaQueryResult(
                     val dataframe : Option[DataFrame]
                     ,val query : String
                    ,val typeQuery : String
                    ,val status : String
                    ,val  error : Option[Exception]
                  )extends Serializable {

  //override def toString(): String = s"Query: " + query + " -- typeQuery: " + typeQuery + " -- Status: " + status


override def toString(): String = {
    if (dataframe.isDefined && error.isDefined ) {
      val toPrint = s"Query Details:\n\tType Query:\t${typeQuery}\n\tStatement:\t${query}\n\tEstatus:\t${status}\n\t+" +
        s"Query Result (20 rows):\n${dataframe.get.show(20)}" +
        s"Error:\n\t\t${error.get} "
      toPrint
    } else if (dataframe.isDefined && error.isEmpty) {
      val toPrint = s"Query Details:\n\tType Query:\t${typeQuery}\n\tStatement:\t${query}\n\tEstatus:\t${status}\n\t+" +
        s"Query Result:\n${dataframe.get}" +
        s"Error:\t None "
      toPrint
    } else {
      val toPrint = s"Query Details:\n\tType Query:\t${typeQuery}\n\tStatement:\t${query}\n\tEstatus:\t${status}\n\t+" +
        s"Query Result (20 rows):\t None" +
        s"Error:\t None "
      toPrint
    }
  }

}
