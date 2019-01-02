package SparkFederation.Lib

import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, SubqueryAlias}
//import org.apache.spark.{SparkConf, SparkContext}

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.catalyst.parser.ParserInterface
import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, SubqueryAlias}
import org.apache.spark.sql.catalyst.analysis.UnresolvedRelation

object SparkProperties {

   final val ss = SparkSession.builder
    .master("local")
    .appName("SparkFederation")
    .config("spark.driver.host","localhost")
    //.config("spark.debug.maxToStringFields", "100")
    .master("local[*]")
    .getOrCreate()

}
