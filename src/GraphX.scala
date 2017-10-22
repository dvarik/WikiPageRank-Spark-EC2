import scala.collection.mutable.ListBuffer

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.graphx.Edge
import org.apache.spark.graphx.Graph
import org.apache.spark.graphx.VertexId
import org.apache.spark.rdd.RDD
import org.jsoup.Jsoup

object GraphX {

  def Hash(title: String): VertexId = {
    title.toLowerCase.replace(" ", "").hashCode.toLong
  }

  def main(args: Array[String]) {

    if (args.length < 3) {
      System.err.println(
        "Args: <inputFile> <outputFile> <Iterations>")
      System.exit(1)
    }

    case class UniSet(val title: String, val links: List[String])

    val input = args(0)
    val conf = new SparkConf().setAppName("GraphX Application")
    val sc = new SparkContext(conf)

    val lines = sc.textFile(input).flatMap(file => file.split("\n"))

    val adjList = lines.map { line =>
      val cols = line.split("\t")
      val xml = Jsoup.parse(cols(3))
      val target = xml.getElementsByTag("target")
      var links = new ListBuffer[String]()
      for (t <- 0 to target.size() - 1) {
        links += target.get(t).text()

      }
      new UniSet(cols(1), links.toList)
    }.cache()
    
    val verts = adjList.map(a => (Hash(a.title), a.title)).cache

    val edges: RDD[Edge[Double]] = adjList.flatMap { a =>
      val src = Hash(a.title)
      a.links.map { v => Edge(src, Hash(v), 1.0)
      }
    }

    val graph = Graph(verts, edges)
    val pr = graph.staticPageRank(2).cache()
    val g = graph.outerJoinVertices(pr.vertices) {
      (v, title, rank) => (rank.getOrElse(0.0), title)
    }

    val output = g.vertices.top(100)
    val res = sc.parallelize(output)
    res.coalesce(1).saveAsTextFile(args(1))
    sc.stop()
  }

}
