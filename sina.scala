/* sinaApp.scala */
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import scala.collection.mutable.Map
import org.apache.spark._
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
object sinaApp {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("sinaApplication").set("spark.executor.memory","4g")
    val sc = new SparkContext(conf)
    //补全职业方向:去除大V用户计算出度边中最多的职业作为自身职业
    if(args(0)=="job")
    {
        val graph=GraphLoader.edgeListFile(sc,args(1))//加载边
        val users=sc.textFile(args(2)).map{line=>//加载定点数据
                val fields=line.split(",")
                (fields(0).toLong,(fields(1),fields(2),fields(3)))//ID,location,job,verified
        }
        //val defaultVertexAttr=("null","null","False")
        val newGraph=Graph.apply(users,graph.edges)//.cache()//修改定点数据结构,若边中点在users中不存在则以defaultVertexAttr(这里是null)赋值
        val myGraph=newGraph.subgraph(triplet=>triplet.srcAttr!=null && triplet.dstAttr!=null,(id,attr)=>attr!=null)//.cache()//取顶点数据非空的子图

        val profile=myGraph.mapReduceTriplets[Map[String,Int]](
            triplet=>{//Map fun
                if(triplet.srcAttr._2=="null" && triplet.dstAttr._3=="False" && triplet.dstAttr._2!="null"){
                    Iterator((triplet.srcId,Map[String,Int](triplet.dstAttr._2->1)))
                }
                else
                    Iterator.empty
            }
            ,
            (a,b)=>{//Reduce fun
                var myMap=Map[String,Int]()
                for((k,v)<-a){
                    if(b.contains(k))
                    {
                        val t=a(k)+b(k)
                        myMap+=(k->t)
                    }
                    else
                        myMap+=(k->a(k))
                }
                myMap
            }
        )
        val profileMax=profile.mapValues[String](
            (id:VertexId,m:Map[String,Int])=>{
                val x=m.values.max
                m.filter(v=>v._2==x).keys.toList(0)
            }
        )
        profileMax.saveAsTextFile(args(3))//结果存至指定目录中
    }

    //补全地理位置:去除大V用户计算出度边中最多的地点作为自身地点
    if(args(0)=="location")
    {
        val graph=GraphLoader.edgeListFile(sc,args(1))//加载边
        val users=sc.textFile(args(2)).map{line=>//加载定点数据
                val fields=line.split(",")
                (fields(0).toLong,(fields(1),fields(2),fields(3)))//ID,location,job,verified
        }
        //val defaultVertexAttr=("null","null","False")
        val newGraph=Graph.apply(users,graph.edges)//.cache()//修改定点数据结构,若边中点在users中不存在则以defaultVertexAttr(这里是null)赋值
        val myGraph=newGraph.subgraph(triplet=>triplet.srcAttr!=null && triplet.dstAttr!=null,(id,attr)=>attr!=null)//.cache()//取顶点数据非空的子图

        val location=myGraph.mapReduceTriplets[Map[String,Int]](
            triplet=>{//Map fun
                if(triplet.srcAttr._1=="null" && triplet.dstAttr._3=="False" && triplet.dstAttr._1!="null")
                    Iterator((triplet.srcId,Map[String,Int](triplet.dstAttr._1->1)))
                else
                    Iterator.empty
            }
            ,
            (a,b)=>{//Reduce fun
                var myMap=Map[String,Int]()
                for((k,v)<-a){
                    if(b.contains(k))
                    {
                        val t=a(k)+b(k)
                        myMap+=(k->t)
                    }
                    else
                        myMap+=(k->a(k))
                }
                myMap
            }
        )
        val locationMax=location.mapValues[String](
            (id:VertexId,m:Map[String,Int])=>{
                val x=m.values.max
                m.filter(v=>v._2==x).keys.toList(0)
            }
        )
        println("######## ",myGraph.vertices.count,myGraph.edges.count,myGraph.triplets.count,location.count,locationMax.count)
        locationMax.saveAsTextFile(args(3))
    }

    //计算好友：地理熵、职业熵、大V
    if(args(0)=="friends")
    {
        val graph=GraphLoader.edgeListFile(sc,args(1))//加载边
        val users=sc.textFile(args(2)).map{line=>//加载定点数据
                val fields=line.split(",")
                (fields(0).toLong,(fields(1),fields(2),fields(3)))//ID,location,job,verified
        }
        //val defaultVertexAttr=("null","null","False")
        val newGraph=Graph.apply(users,graph.edges)//.cache()//修改定点数据结构,若边中点在users中不存在则以defaultVertexAttr(这里是null)赋值
        val myGraph=newGraph.subgraph(triplet=>triplet.srcAttr!=null && triplet.dstAttr!=null,(id,attr)=>attr!=null)//.cache()//取顶点数据非空的子图

        val friends=myGraph.mapReduceTriplets[(Map[String,Double],Map[String,Double],Double,Double)](//好友地域，好友职业，大V好友数目,好友数目
            triplet=>{//Map fun
                if(triplet.dstAttr._3=="True")
                    Iterator((triplet.srcId,(Map[String,Double](triplet.dstAttr._1->0),Map[String,Double](triplet.dstAttr._2->0),1,1)))
                else
                    Iterator((triplet.srcId,(Map[String,Double](triplet.dstAttr._1->1),Map[String,Double](triplet.dstAttr._2->1),0,1)))
            }
            ,
            (a,b)=>{//Reduce fun
                var locMap=Map[String,Double]()
                for((k,v)<-a._1){
                    if(b._1.contains(k))
                    {
                        val t=a._1(k)+b._1(k)
                        locMap+=(k->t)
                    }
                    else
                        locMap+=(k->a._1(k))
                }
                var jobMap=Map[String,Double]()
                for((k,v)<-a._2){
                    if(b._2.contains(k))
                    {
                        val t=a._2(k)+b._2(k)
                        jobMap+=(k->t)
                    }
                    else
                        jobMap+=(k->a._2(k))
                }
                (locMap,jobMap,a._3+b._3,a._4+b._4)
            }
        )
        val friendsValue=friends.mapValues[Double](
            (id:VertexId,attr:(Map[String,Double],Map[String,Double],Double,Double))=>{
                val sumLoc=attr._1.values.sum
                val dLoc:Double=attr._1.values.map(a=>{if(a>0) a/sumLoc*Math.log(a/sumLoc) else 0}).reduce((a,b)=>a+b)
                val sumJob=attr._2.values.sum
                val dJob:Double=attr._2.values.map(a=>if(a>0) a/sumJob*Math.log(a/sumJob) else 0).reduce((a,b)=>a+b)
                val dV:Double=attr._3/(attr._4+1.0)
                -dLoc-dJob+dV
            }
        )
        println("######## ",myGraph.vertices.count,myGraph.edges.count,myGraph.triplets.count,friends.count,friendsValue.count)
        friendsValue.saveAsTextFile(args(3))
    }

    //计算网络pageRank值
    if(args(0)=="pageRank"){
        val graph=GraphLoader.edgeListFile(sc,args(1))//加载边
        val ranks=graph.pageRank(0.1).vertices
        println("######## ",graph.vertices.count,ranks.count)
        ranks.saveAsTextFile(args(2))
    }
  }
}
