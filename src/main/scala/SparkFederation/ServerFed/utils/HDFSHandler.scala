package SparkFederation.ServerFed.utils

import SparkFederation.Exceptions.TableNoExistFed
import SparkFederation.Lib.{HDFSProperties, ZooKeeperProperties}
import SparkFederation.ServerFed.zkCoordinatorFed.zkExecutor
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, SparkSession}

class HDFSHandler  (implicit ss: SparkSession) {

  val zk = new zkExecutor ()

  def csv2Parquet(
                   inFilename : String
                   ,outFilename: String
                   ,customSchema: StructType = null
                   ,delimiter: String = ","
                   ,scape: String = "\""
                   ,header: String = "true"
                   ,multiline: String = "true"
                   ,dateFormat: String = "yyyMMdd"

                 )
  : Unit = {


    val df = ss.read.schema(customSchema)
      .option("header", header)
      .option("dateFormat", dateFormat)
      .option("delimiter",delimiter)
      .option("inferSchema", "false")
      .option("escape", scape)
      .option("multiline",multiline)
      .csv(HDFSProperties.HADOOP_RAW + inFilename )

    df.show()

    // Write file to parquet
    val parquetFile = HDFSProperties.HADOOP_DATA + outFilename
    df.write.parquet(parquetFile)

  }

  def readParquet(filename: String) : Unit = {

    val pathfile = HDFSProperties.HADOOP_DATA + filename
    // read back parquet to DF
    val newDataDF = ss.read.parquet(pathfile)

    // show contents
    newDataDF.show()
  }

  @throws(classOf[TableNoExistFed])
  def getDataset(hdfsPath: String)  : DataFrame = {

    try {
      // read back parquet to DF
      val newDataDF = ss.read.parquet(hdfsPath)
      // return dataset
      newDataDF

    } catch {

      case e : org.apache.spark.sql.AnalysisException =>
        println (e.getMessage + " " + e.getCause)
        throw new TableNoExistFed("Table: " + hdfsPath + "doesn't exist", e.getCause )
    }
  }


  def iniDatasets () : Unit = {

    // https://stackoverflow.com/questions/50050016/how-to-create-an-external-spark-table-from-data-in-hdfs
    val schemaCC: StructType = StructType(Array(
      StructField("Date_received",DateType,nullable = true)
      ,StructField("Product",StringType,nullable = true)
      ,StructField("Subproduct",StringType,nullable = true)
      ,StructField("Issue",StringType,nullable = true)
      ,StructField("Subissue",StringType,nullable = true)
      ,StructField("Consumer_complaint_narrative",StringType,nullable = true)
      ,StructField("Company_public_response",StringType,nullable = true)
      ,StructField("Company",StringType,nullable = true)
      ,StructField("State",StringType,nullable = true)
      ,StructField("ZIP_code",StringType,nullable = true)
      ,StructField("Tags",StringType,nullable = true)
      ,StructField("Consumer_consent_provided",StringType,nullable = true)
      ,StructField("Submitted_via",StringType,nullable = true)
      ,StructField("Date_sent_to_company",DateType,nullable = true)
      ,StructField("Company_response_to_consumer",StringType,nullable = true)
      ,StructField("Timely_response",StringType,nullable = true)
      ,StructField("Consumer_disputed",StringType,nullable = true)
      ,StructField("Complaint_ID",StringType,nullable = true)))
    val schemaDSbZ: StructType = StructType(Array(
      StructField("JURISDICTION_NAME",IntegerType,nullable = true)
      ,StructField("COUNT_PARTICIPANTS",IntegerType,nullable = true)
      ,StructField("COUNT_FEMALE",IntegerType,nullable = true)
      ,StructField("PERCENT_FEMALE",FloatType,nullable = true)
      ,StructField("COUNT_MALE",IntegerType,nullable = true)
      ,StructField("PERCENT_MALE",FloatType,nullable = true)
      ,StructField("COUNT_GENDER_UNKNOWN",IntegerType,nullable = true)
      ,StructField("PERCENT_GENDER_UNKNOWN",FloatType,nullable = true)
      ,StructField("COUNT_GENDER_TOTAL",IntegerType,nullable = true)
      ,StructField("PERCENT_GENDER_TOTAL",FloatType,nullable = true)
      ,StructField("COUNT_PACIFIC_ISLANDER",IntegerType,nullable = true)
      ,StructField("PERCENT_PACIFIC_ISLANDER",FloatType,nullable = true)
      ,StructField("COUNT_HISPANIC_LATINO",IntegerType,nullable = true)
      ,StructField("PERCENT_HISPANIC_LATINO",FloatType,nullable = true)
      ,StructField("COUNT_AMERICAN_INDIAN",IntegerType,nullable = true)
      ,StructField("PERCENT_AMERICAN_INDIAN",FloatType,nullable = true)
      ,StructField("COUNT_ASIAN_NON_HISPANIC",IntegerType,nullable = true)
      ,StructField("PERCENT_ASIAN_NON_HISPANIC",FloatType,nullable = true)
      ,StructField("COUNT_WHITE_NON_HISPANIC",IntegerType,nullable = true)
      ,StructField("PERCENT_WHITE_NON_HISPANIC",FloatType,nullable = true)
      ,StructField("COUNT_BLACK_NON_HISPANIC",IntegerType,nullable = true)
      ,StructField("PERCENT_BLACK_NON_HISPANIC",FloatType,nullable = true)
      ,StructField("COUNT_OTHER_ETHNICITY",IntegerType,nullable = true)
      ,StructField("PERCENT_OTHER_ETHNICITY",FloatType,nullable = true)
      ,StructField("COUNT_ETHNICITY_UNKNOWN",IntegerType,nullable = true)
      ,StructField("PERCENT_ETHNICITY_UNKNOWN",FloatType,nullable = true)
      ,StructField("COUNT_ETHNICITY_TOTAL",IntegerType,nullable = true)
      ,StructField("PERCENT_ETHNICITY_TOTAL",IntegerType,nullable = true)
      ,StructField("COUNT_PERMANENT_RESIDENT_ALIEN",IntegerType,nullable = true)
      ,StructField("PERCENT_PERMANENT_RESIDENT_ALIEN",FloatType,nullable = true)
      ,StructField("COUNT_US_CITIZEN",IntegerType,nullable = true)
      ,StructField("PERCENT_US_CITIZEN",FloatType,nullable = true)
      ,StructField("COUNT_OTHER_CITIZEN_STATUS",IntegerType,nullable = true)
      ,StructField("PERCENT_OTHER_CITIZEN_STATUS",FloatType,nullable = true)
      ,StructField("COUNT_CITIZEN_STATUS_UNKNOWN",IntegerType,nullable = true)
      ,StructField("PERCENT_CITIZEN_STATUS_UNKNOWN",FloatType,nullable = true)
      ,StructField("COUNT_CITIZEN_STATUS_TOTAL",IntegerType,nullable = true)
      ,StructField("PERCENT_CITIZEN_STATUS_TOTAL",FloatType,nullable = true)
      ,StructField("COUNT_RECEIVES_PUBLIC_ASSISTANCE",IntegerType,nullable = true)
      ,StructField("PERCENT_RECEIVES_PUBLIC_ASSISTANCE",FloatType,nullable = true)
      ,StructField("COUNT_NRECEIVES_PUBLIC_ASSISTANCE",IntegerType,nullable = true)
      ,StructField("PERCENT_NRECEIVES_PUBLIC_ASSISTANCE",FloatType,nullable = true)
      ,StructField("COUNT_PUBLIC_ASSISTANCE_UNKNOWN",IntegerType,nullable = true)
      ,StructField("PERCENT_PUBLIC_ASSISTANCE_UNKNOWN",FloatType,nullable = true)
      ,StructField("COUNT_PUBLIC_ASSISTANCE_TOTAL",IntegerType,nullable = true)
      ,StructField("PERCENT_PUBLIC_ASSISTANCE_TOTAL",FloatType,nullable = true)
    ))

    val createDemografic = s"CREATE TABLE Demographic_Statistics_By_Zip_Code (JURISDICTION_NAME INT, COUNT_PARTICIPANTS INT, COUNT_FEMALE INT, PERCENT_FEMALE FLOAT, COUNT_MALE INT, PERCENT_MALE FLOAT, COUNT_GENDER_UNKNOWN INT, PERCENT_GENDER_UNKNOWN FLOAT, COUNT_GENDER_TOTAL INT, PERCENT_GENDER_TOTAL FLOAT, COUNT_PACIFIC_ISLANDER INT, PERCENT_PACIFIC_ISLANDER FLOAT, COUNT_HISPANIC_LATINO INT, PERCENT_HISPANIC_LATINO FLOAT, COUNT_AMERICAN_INDIAN INT, PERCENT_AMERICAN_INDIAN FLOAT, COUNT_ASIAN_NON_HISPANIC INT, PERCENT_ASIAN_NON_HISPANIC FLOAT, COUNT_WHITE_NON_HISPANIC INT, PERCENT_WHITE_NON_HISPANIC FLOAT, COUNT_BLACK_NON_HISPANIC INT, PERCENT_BLACK_NON_HISPANIC FLOAT, COUNT_OTHER_ETHNICITY INT, PERCENT_OTHER_ETHNICITY FLOAT, COUNT_ETHNICITY_UNKNOWN INT, PERCENT_ETHNICITY_UNKNOWN FLOAT, COUNT_ETHNICITY_TOTAL INT, PERCENT_ETHNICITY_TOTAL INT, COUNT_PERMANENT_RESIDENT_ALIEN INT, PERCENT_PERMANENT_RESIDENT_ALIEN FLOAT, COUNT_US_CITIZEN INT, PERCENT_US_CITIZEN FLOAT, COUNT_OTHER_CITIZEN_STATUS INT, PERCENT_OTHER_CITIZEN_STATUS FLOAT, COUNT_CITIZEN_STATUS_UNKNOWN INT, PERCENT_CITIZEN_STATUS_UNKNOWN FLOAT, COUNT_CITIZEN_STATUS_TOTAL INT, PERCENT_CITIZEN_STATUS_TOTAL FLOAT, COUNT_RECEIVES_PUBLIC_ASSISTANCE INT, PERCENT_RECEIVES_PUBLIC_ASSISTANCE FLOAT, COUNT_NRECEIVES_PUBLIC_ASSISTANCE INT, PERCENT_NRECEIVES_PUBLIC_ASSISTANCE FLOAT, COUNT_PUBLIC_ASSISTANCE_UNKNOWN INT, PERCENT_PUBLIC_ASSISTANCE_UNKNOWN FLOAT, COUNT_PUBLIC_ASSISTANCE_TOTAL INT, PERCENT_PUBLIC_ASSISTANCE_TOTAL FLOAT) USING parquet OPTIONS ( path '${HDFSProperties.HADOOP_DATA}Demographic_Statistics_By_Zip_Code')"
    val createConsumer = s"CREATE TABLE Consumer_Complaints (Date_received DATE, Product STRING, Subproduct STRING, Issue STRING, Subissue STRING, Consumer_complaint_narrative STRING, Company_public_response STRING, Company STRING, State STRING, ZIP_code STRING, Tags STRING, Consumer_consent_provided STRING, Submitted_via STRING, Date_sent_to_company DATE, Company_response_to_consumer STRING, Timely_response STRING, Consumer_disputed STRING, Complaint_ID STRING) USING parquet OPTIONS (  path '${HDFSProperties.HADOOP_DATA}Consumer_Complaints')"
    val serverExecutor = new ServerHandler()

    this.csv2Parquet(
      "Consumer_Complaints.csv"
      , "Consumer_Complaints"
      ,schemaCC
      ,dateFormat = "MM/dd/yyyy")

    this.csv2Parquet(
      "Demographic_Statistics_By_Zip_Code.csv"
      , "Demographic_Statistics_By_Zip_Code"
      ,schemaDSbZ)

  }

}
