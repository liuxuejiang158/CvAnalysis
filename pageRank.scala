/* pageRankApp.scala */
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import scala.collection.mutable.Map
import org.apache.spark._
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
object pageRankApp {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("sinaApplication")
    val sc = new SparkContext(conf)
    val graph=GraphLoader.edgeListFile(sc,"/home/spark/spark/apps/sina/edge.txt")//加载边
    val ranks = graph.pageRank(0.0001).vertices
    ranks.saveAsTextFile("/home/spark/spark/apps/sina/pageRank")
}
