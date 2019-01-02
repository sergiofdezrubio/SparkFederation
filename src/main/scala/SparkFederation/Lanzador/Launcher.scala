package SparkFederation.Lanzador


import java.io.{ByteArrayInputStream, ObjectInputStream}
import java.time.Duration
import java.util.{Collections, Properties}

import org.apache.zookeeper.ZooKeeper
import SparkFederation.ConnectorsFed.{KafkaClientMessage, KafkaConsumerFed, KafkaProducerFed}
import SparkFederation.Lib.{HDFSProperties, KafkaProperties, SparkProperties}
import org.apache.kafka.clients.admin.{AdminClient, AdminClientConfig, ListTopicsOptions}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.clients.consumer.ConsumerRecords

import scala.collection.JavaConversions._
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, Project, SubqueryAlias}
import java.util.Properties

import SparkFederation.ClientFed.SimpleClientFed
import SparkFederation.ServerFed.SimpleServerFed
import SparkFederation.ServerFed.utils.{HDFSHandler, ServerHandler, SparkSQLHandler}
import SparkFederation.ServerFed.zkCoordinatorFed.zkExecutor
import kafka.zookeeper.ZooKeeperClient
import org.apache.spark.sql.catalyst.analysis.UnresolvedRelation
import org.apache.spark.sql.execution.command.DropTableCommand
import org.apache.spark.sql.execution.datasources.CreateTable
import org.apache.zookeeper.Watcher
//import org.apache.spark.sql.types.{BooleanType, DateType, IntegerType, StringType}
import org.apache.spark.sql.types._

//import org.apache.avro.generic.GenericData.StringType
import org.apache.kafka.clients.admin.{AdminClient, ListTopicsOptions, NewTopic}

import scala.collection.JavaConverters._
import org.apache.kafka.clients.admin.AdminClientConfig
import org.apache.spark
import org.apache.spark.sql.types.{StructField, StructType}


// http://localhost:50070/
//kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic SummitQuery -from-beginning
//
//
/*
Datasets:
  First choice:
    - https://catalog.data.gov/dataset/consumer-complaint-database
    - https://catalog.data.gov/dataset/demographic-statistics-by-zip-code-acfc9
   Second Choice:
    - http://insideairbnb.com/get-the-data.html

HDFS Paths:
    Home Proyect:
    - /user/utad/workspace/SparkFederation

    Datasets
    - /user/utad/workspace/SparkFederation




*/

object Launcher extends App {

  /*
  def getTables(query: String): Seq[String] = {

    val sparkSession = SparkSession.builder
      .master("local")
      .appName("my-spark-app")
      .config("spark.driver.host","localhost")
      .master("local[*]")
      .getOrCreate()
    val logicalPlan = sparkSession.sessionState.sqlParser.parsePlan(query)
    import org.apache.spark.sql.catalyst.analysis.UnresolvedRelation
    logicalPlan.collect { case r: UnresolvedRelation => r.tableName  }


  }

  def getQueryPlan(query: String): Seq[(String,LogicalPlan)] = {

    val sparkSession = SparkSession.builder
      .master("local")
      .appName("my-spark-app")
      .config("spark.driver.host","localhost")
      .master("local[*]")
      .getOrCreate()
    val logicalPlan = sparkSession.sessionState.sqlParser.parsePlan(query)
    println (logicalPlan)
    logicalPlan.collect{ case r : SubqueryAlias => (r.alias,r.child)  }
  }
*/

  override def main(args : Array[String]): Unit = {
    /*
    val query = "select * from table_1 as a left join table_2 as b on a.id=b.id where a.id = 2"
    /*
    Launcher.getTables(query).foreach(println)

    val plan =Launcher.getQueryPlan(query)

    plan.foreach( tupla  => { println ( tupla._1 + " -- " + tupla._2) })
    */

    val exProd = new KafkaProducerFed[KafkaClientMessage](  "ClientQuery_1","query",
                                       "SparkFederation.ConnectorsFed.KafkaClientSerializer")
    val exCons = new KafkaConsumerFed[KafkaClientMessage]("severGroup_1","query",
                                      "SparkFederation.ConnectorsFed.KafkaClientDeserializer")

    val data = new KafkaClientMessage("topic_1", query)
    println("antes de enviar mensaje")

      exProd.sendMessage(data)



    println("despues de enviar mensaje")
    val exConsumer = exCons.consumer
    println("antes de consumir")

    var flag = 0
    while (flag == 0) {

      val records = exConsumer.poll(100)

      println("Antes bucle " + records.count() + " vacio: " + records.isEmpty())

      for (record <- records.iterator()) {
        println("entro")
        println("Received message: (" + record.key() + ", " + record.value() + ") at offset " + record.offset())
        flag = 1
      }

    }
*/
    /*
    val KafkaServer = "localhost:9092"
    val topic = "default"
    val zookeeperConnect = KafkaServer
    val sessionTimeoutMs = 10 * 1000
    val connectionTimeoutMs = 8 * 1000

    val partitions = 1
    val replication:Short = 1
    val topicConfig = new Properties() // add per-topic configurations settings here

    val config = new Properties
    config.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, KafkaServer)
    val admin = AdminClient.create(config)

    admin.deleteTopics(List(topic).asJavaCollection)


    val existing1 = admin.listTopics(new ListTopicsOptions().timeoutMs(500).listInternal(true))
    val nms1 = existing1.names()
    //nms1.get().asScala.foreach(nm => println(nm)) // nms.get() fails


    val newTopic = new NewTopic(topic, partitions, replication)
    newTopic.configs(Map[String,String]().asJava)
    val ret = admin.createTopics(List(newTopic).asJavaCollection)
    //ret.all().get() // Also fails
    val a = List[String]()


    //println ("-- " + ret.values().keySet().iterator().next())
    println ("--1 " + ret.values().asScala.keys.head)


    val existing = admin.listTopics(new ListTopicsOptions().timeoutMs(500).listInternal(true))
    val nms = existing.names()
   // nms.get().asScala.foreach(nm => println(nm)) // nms.get() fails


    admin.close()
    */
    /*
    //KafkaProperties.deleteTopic("ClientTopic_1")
    //KafkaProperties.deleteTopic("SummitQuery")
    //KafkaProperties.createTopic("SummitQuery")
    val query = "select * from table_1 as a left join table_2 as b on a.id=b.id where a.id = 3"
    val client = new SimpleClientFed("client1","group1")
    val server = new SimpleServerFed("server","serverCluster")
    try {

      client.summitQuery(query)

      server.listenQuery()

      client.shutdown()
    } catch {
      case e : Exception => {
        client.shutdown()
        println (e.toString)
      }

    }
    */
    /*
    val customSchemaDemografic = StructType(Array(
      StructField("id", StringType, nullable = false),
      StructField("flag1", BooleanType, nullable = false),
      StructField("flag2", BooleanType, nullable = false),
      StructField("flag3", BooleanType, nullable = false),
      StructField("flag4", BooleanType, nullable = false),
      StructField("flag6", BooleanType, nullable = false))

    )

    HDFSProperties.csv2Parquet(SparkProperties.spark
      , "Demographic_Statistics_By_Zip_Code.csv"
      , "Demographic_Statistics_By_Zip_Code.parquet"
      ,customSchemaDemografic
    )
*//*
    val spark = SparkSession.builder
      .master("local")
      .appName("SparkFederationServer")
      .config("spark.driver.host","localhost")
      .master("local[*]")
      .getOrCreate()
*/
    /*
    val customSchemaConsumer = StructType(Array(
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
        ,StructField("Complaint_ID",StringType,nullable = true))

    )
*/
    /*
    HDFSProperties.csv2Parquet(spark
      , "Consumer_Complaints.csv"
      , "Consumer_Complaints.parquet"
      ,customSchemaConsumer
      ,dateFormat = "MM/dd/yyyy"
      ,scape = "\"")
*/
    /*
    //HDFSProperties.readParquet(SparkProperties.spark, "Consumer_Complaints.parquet")
    HDFSProperties.csv2Parquet(spark
      , "Demographic_Statistics_By_Zip_Code.csv"
      , "Demographic_Statistics_By_Zip_Code.parquet"
      ,customSchemaConsumer
      ,dateFormat = "MM/dd/yyyy"
      ,scape = "\"")

    HDFSProperties.readParquet(SparkProperties.spark, "Demographic_Statistics_By_Zip_Code.parquet")


    val customSchemaConsumer = StructType(Array(
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

    HDFSProperties.csv2Parquet(SparkProperties.spark
      , "Demographic_Statistics_By_Zip_Code.csv"
      , "Demographic_Statistics_By_Zip_Code.parquet"
      ,customSchemaConsumer
      ,scape = "\"")

    HDFSProperties.readParquet(SparkProperties.spark, "Demographic_Statistics_By_Zip_Code.parquet")

    val df = SparkProperties.spark.read.schema(customSchemaConsumer)
      .option("header", true)
      .option("dateFormat", "yyyMMdd")
      .option("delimiter",",")
      .option("inferSchema", "false")
      .option("escape", "\"")
      .option("multiline","true")
      .csv(HDFSProperties.HADOOP_RAW + "Demographic_Statistics_By_Zip_Code.csv" )

    df.show()
    */
    /*
    HDFSProperties.csv2Parquet(
      "Consumer_Complaints.csv"
      , "Consumer_Complaints.parquet"
      ,schemaCC
      ,dateFormat = "MM/dd/yyyy")*/
    /*
    //val hdfsMaster = new HDFSHandler()
    //hdfsMaster.iniDatasets()
    /*println ("Antes-----")
    HDFSProperties.readParquet("Demographic_Statistics_By_Zip_Code.parquet")
    println("Despues ---- ")
    val tabla = SparkProperties.getHDFSTable("Demographic_Statistics_By_Zip_Code")
    println("despues 2 ***")
    tabla.show()

    println("despues 3 ***")
    val tabla1 = SparkProperties.getHDFSTable("Demographic_Statistics_By_Zip_Code")
    println("despues 3 ***")
    tabla1.show()
    */

    //val zkMaster = new zkExecutor()

    //zkMaster.createZnode("/hdfs/prueba", "Esto es una prueba".getBytes)


    //val rawData = zkMaster.getData("/hdfs/prueba").get
    //val netData = (rawData.map(_.toChar)).mkString
    //println(netData)

    //zkMaster.setData("/hdfs/prueba", "Esto es una prueba 2".getBytes)

    //zkMaster.deleteZnode("/hdfs/prueba")
*/
    /*
        val SQLHand = new SparkSQLHandler(spHandler)
        val coreServer = new ServerHandler()

        //val query = "select * from Demographic_Statistics_By_Zip_Code"
        val query = "select b.JURISDICTION_NAME,a.ZIP_code,a.Complaint_ID from Consumer_Complaints a inner join Demographic_Statistics_By_Zip_Code b on a.ZIP_code = b.JURISDICTION_NAME"

        val tableNames = SQLHand.getQueryTableNames(query)

        println(s"Tablenames from query: ${query} ")

        println ("Register Tables")
        SQLHand.getQueryTables(tableNames)

        println("Query Execution")
        session.sql(query).show
    */
    /*
        val df = session.read.parquet("hdfs://127.0.0.1:9000/user/utad/workspace/SparkFederation/data/Consumer_Complaints.parquet")

        df.limit(1).write.option("path", "hdfs://127.0.0.1:9000/Consumer_Complaints").
          saveAsTable("Consumer_Complaints")



        session.sql("SHOW CREATE TABLE Consumer_Complaints").show(1, false)
  */

    implicit val session = SparkProperties.ss

    //val hdfsHandler = new HDFSHandler()
    val serverExecutor = new ServerHandler()

    //hdfsHandler.iniDatasets()
    //serverExecutor.initServer


    /*
    println("**************** Se consultan las tablas")
    session.sql("select * from Demographic_Statistics_By_Zip_Code").show
    session.sql("select * from Consumer_Complaints").show
    */

    val sqlExecutor = new SparkSQLHandler(serverExecutor)
    // CreateDataSourceTableCommand
    val createDemografic = s"CREATE TABLE db1.Demographic_Statistics_By_Zip_Code (JURISDICTION_NAME INT, COUNT_PARTICIPANTS INT, COUNT_FEMALE INT, PERCENT_FEMALE FLOAT, COUNT_MALE INT, PERCENT_MALE FLOAT, COUNT_GENDER_UNKNOWN INT, PERCENT_GENDER_UNKNOWN FLOAT, COUNT_GENDER_TOTAL INT, PERCENT_GENDER_TOTAL FLOAT, COUNT_PACIFIC_ISLANDER INT, PERCENT_PACIFIC_ISLANDER FLOAT, COUNT_HISPANIC_LATINO INT, PERCENT_HISPANIC_LATINO FLOAT, COUNT_AMERICAN_INDIAN INT, PERCENT_AMERICAN_INDIAN FLOAT, COUNT_ASIAN_NON_HISPANIC INT, PERCENT_ASIAN_NON_HISPANIC FLOAT, COUNT_WHITE_NON_HISPANIC INT, PERCENT_WHITE_NON_HISPANIC FLOAT, COUNT_BLACK_NON_HISPANIC INT, PERCENT_BLACK_NON_HISPANIC FLOAT, COUNT_OTHER_ETHNICITY INT, PERCENT_OTHER_ETHNICITY FLOAT, COUNT_ETHNICITY_UNKNOWN INT, PERCENT_ETHNICITY_UNKNOWN FLOAT, COUNT_ETHNICITY_TOTAL INT, PERCENT_ETHNICITY_TOTAL INT, COUNT_PERMANENT_RESIDENT_ALIEN INT, PERCENT_PERMANENT_RESIDENT_ALIEN FLOAT, COUNT_US_CITIZEN INT, PERCENT_US_CITIZEN FLOAT, COUNT_OTHER_CITIZEN_STATUS INT, PERCENT_OTHER_CITIZEN_STATUS FLOAT, COUNT_CITIZEN_STATUS_UNKNOWN INT, PERCENT_CITIZEN_STATUS_UNKNOWN FLOAT, COUNT_CITIZEN_STATUS_TOTAL INT, PERCENT_CITIZEN_STATUS_TOTAL FLOAT, COUNT_RECEIVES_PUBLIC_ASSISTANCE INT, PERCENT_RECEIVES_PUBLIC_ASSISTANCE FLOAT, COUNT_NRECEIVES_PUBLIC_ASSISTANCE INT, PERCENT_NRECEIVES_PUBLIC_ASSISTANCE FLOAT, COUNT_PUBLIC_ASSISTANCE_UNKNOWN INT, PERCENT_PUBLIC_ASSISTANCE_UNKNOWN FLOAT, COUNT_PUBLIC_ASSISTANCE_TOTAL INT, PERCENT_PUBLIC_ASSISTANCE_TOTAL FLOAT) USING parquet OPTIONS ( path '${HDFSProperties.HADOOP_DATA}Demographic_Statistics_By_Zip_Code')"
    val dropDemografic = "DROP TABLE Demographic_Statistics_By_Zip_Code"
    val selectDemo = "select a from Demographic_Statistics_By_Zip_Code"
    val create1 = "create table caca as select JURISDICTION_NAME from  Demographic_Statistics_By_Zip_Code  "
/*
    println("Explain plan physical")
    //println(sqlExecutor.getQueryPlan(createDemografic).toString())
    session.sql(createDemografic).explain()
    session.sql(dropDemografic).explain()
*/
    println("Explain plan logical")
    //ss.sessionState.sqlParser.parsePlan(query)

    /*
      DropTableCommand `Demographic_Statistics_By_Zip_Code`, false, false, false

      'CreateTable `Demographic_Statistics_By_Zip_Code`, ErrorIfExists

      'Project ['a]
      +- 'UnresolvedRelation `Demographic_Statistics_By_Zip_Code`

      'CreateTable `caca`, org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe, ErrorIfExists
      +- 'Project ['JURISDICTION_NAME]
         +- 'UnresolvedRelation `Demographic_Statistics_By_Zip_Code
    */

    val lp0 = session.sessionState.sqlParser.parsePlan(createDemografic)
    //val table = lp0.collect { case r: CreateTable => r.tableDesc.identifier.table}
    val table = lp0.collectFirst { case r: CreateTable => r.tableDesc.identifier.table}
    //println(table.get(0))
    println(table.get)

    val lp1 = session.sessionState.sqlParser.parsePlan(dropDemografic)
    //val table1 = lp1.collect { case r: DropTableCommand => r.tableName.table}
    val table1 = lp1.collectFirst { case r: DropTableCommand => r.tableName.table}
    //println(table1.get(0))
    println(table1.get)

    val lp2 = session.sessionState.sqlParser.parsePlan(selectDemo)
    //val table2 = lp2.collect { case r: DropTableCommand => r.tableName.table}
    val table3 = lp2.collectFirst{ case r: Project => { lp2.collect { case r: UnresolvedRelation => r.tableName} }}
    println(table3.get.get(0))


    val lp3 = session.sessionState.sqlParser.parsePlan(create1)
    val table4 = lp3.collectFirst { case r: CreateTable => r.tableDesc.identifier.table}
    println(table4.get)



  }

}
