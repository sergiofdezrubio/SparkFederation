package SparkFederation.ServerFed.utils

import SparkFederation.Exceptions.TableNoExistHDFS
import SparkFederation.Lib.{HDFSProperties, ZooKeeperProperties}
import SparkFederation.ServerFed.zkCoordinatorFed.zkExecutor
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, SparkSession}

class HDFSHandler {

  val zk = new zkExecutor ()

  // hdfs dfs -mkdir -p /user/utad/workspace/SparkFederation/data/raw/
  // hdfs dfs -put *.csv /user/utad/workspace/SparkFederation/data/raw/

  def csv2Parquet(
                   inFilename : String
                   ,outFilename: String
                   ,customSchema: StructType
                   ,delimiter: String = ","
                   ,scape: String = "\""
                   ,header: String = "true"
                   ,multiline: String = "true"
                   ,dateFormat: String = "yyyMMdd"

                 )
                 (implicit spark: SparkSession)
  : Unit = {


    val df = spark.read.schema(customSchema)
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
    df.write.parquet(parquetFile )

  }

  def readParquet(filename: String) (implicit spark: SparkSession): Unit = {


    val pathfile = HDFSProperties.HADOOP_DATA + filename
    // read back parquet to DF
    val newDataDF = spark.read.parquet(pathfile)

    // show contents
    newDataDF.show()


  }

  @throws(classOf[TableNoExistHDFS])
  def getDataset(hdfsPath: String) (implicit ss: SparkSession)  : DataFrame = {


    try {
      // read back parquet to DF
      val newDataDF = ss.read.parquet(hdfsPath)
      // return dataset
      newDataDF

    } catch {

      case e : org.apache.spark.sql.AnalysisException =>
        println (e.getMessage + " " + e.getCause)
        throw new TableNoExistHDFS("Table: " + hdfsPath + "doesn't exist", e.getCause )
      //null

    }


  }

  def registerDataset(name : String , hdfsPath: String ) : Unit = {
    this.zk.createZnode(ZooKeeperProperties.ZNODE_HDFS + "/" + name , hdfsPath.getBytes )
  }

  def iniDatasets () (implicit spark: SparkSession) : Unit = {

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

    this.csv2Parquet(
      "Consumer_Complaints.csv"
      , "Consumer_Complaints.parquet"
      ,schemaCC
      ,dateFormat = "MM/dd/yyyy")

    this.readParquet("Consumer_Complaints.parquet")

    this.registerDataset("Consumer_Complaints", HDFSProperties.HADOOP_DATA +  "Consumer_Complaints.parquet")

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

    this.csv2Parquet(
      "Demographic_Statistics_By_Zip_Code.csv"
      , "Demographic_Statistics_By_Zip_Code.parquet"
      ,schemaDSbZ)

    this.readParquet("Demographic_Statistics_By_Zip_Code.parquet")

    this.registerDataset("Demographic_Statistics_By_Zip_Code", HDFSProperties.HADOOP_DATA  + "Demographic_Statistics_By_Zip_Code.parquet")
  }

}
