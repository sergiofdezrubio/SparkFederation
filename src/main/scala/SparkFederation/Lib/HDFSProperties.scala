package SparkFederation.Lib

import org.apache.spark.SparkContext
import org.apache.spark.sql.{DataFrame, SQLContext, SparkSession}

object HDFSProperties {

  val HADOOP_HOME="/user/utad/workspace/SparkFederation"
  val HADOOP_URY="hdfs://127.0.0.1:9000"
  val HADOOP_DATA="hdfs://127.0.0.1:9000/user/utad/workspace/SparkFederation/data/"
  val HADOOP_RAW= "hdfs://127.0.0.1:9000/user/utad/workspace/SparkFederation/data/raw/"


  def csv2Parquet(
                   spark: SparkSession
                   ,inFilename : String
                   ,outFilename: String
                   ,delimiter: String = ","
                 ) = {
    // Read file as RDD
    val rdd = spark.read.format("csv")
                .option("header", "true")
                .option("delimiter",delimiter)
                .load(this.HADOOP_RAW + inFilename )
    // Convert rdd to data frame using toDF; the following import is required to use toDF function.
    val df: DataFrame = rdd.toDF()
    println ("--- " + this.HADOOP_RAW + " -- " + this.HADOOP_DATA)
    // Write file to parquet
    val parquetFile = this.HADOOP_DATA + outFilename
    df.write.parquet(parquetFile );

  }

  def readParquet(spark: SparkSession,filename: String) = {
    val pathfile = this.HADOOP_DATA + filename
    // read back parquet to DF
    val newDataDF = spark.read.parquet(pathfile)
    // show contents
    newDataDF.show()
  }

}
