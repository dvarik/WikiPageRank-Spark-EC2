import scala.collection.mutable.ListBuffer

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.jsoup.Jsoup

object UniversityPR {
  
  def main(args: Array[String]) {
    if (args.length < 3) {
      System.err.println(
        "Args: <inputFile> <outputFile> <No of iterations> <university-file>")
      System.exit(1)
    }

    val input = args(0)
    val conf = new SparkConf().setAppName("University PageRank Application")
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
      (cols(1), links.toList)
    }.cache()
    
    var ranks = adjList.mapValues(v => 1.0)

    val iters = args(2).toInt

    for (i <- 1 to iters) {
      val contribs = adjList.join(ranks).values.flatMap {
        case (urls, rank) =>
          val size = urls.size
          urls.map(url => (url, rank / size))
      }
      ranks = contribs.reduceByKey(_ + _).mapValues(0.15 + 0.85 * _)
    }
    
    val output = sc.textFile(args(3))
    val u = output.map(s => (s, 1))
    val temp = ranks.join(u).map(item => item.swap).sortByKey(false, 1).take(100)

    val res = sc.parallelize(temp)
    res.coalesce(1).saveAsTextFile(args(1))
    sc.stop()
  }
}
