package SparkFederation.Exceptions

class TableNoExistHDFS(private val message: String = "Table not Exist in HDFS",
                       private val cause: Throwable = None.orNull)
  extends java.lang.NullPointerException
