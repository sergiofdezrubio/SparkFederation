package SparkFederation.Exceptions

class UpdateMetaDataException(private val message: String = "Cannot Update Metadata in Zookeeper",
                              private val cause: Throwable = None.orNull)
  extends java.lang.Exception


