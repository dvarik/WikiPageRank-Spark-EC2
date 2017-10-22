import scala.collection.mutable.ListBuffer

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.jsoup.Jsoup

object PageRank {
  def main(args: Array[String]) {
    if (args.length < 3) {
      System.err.println(
        "Args: <inputFile> <outputFile> <No of iterations>")
      System.exit(1)
    }

    val input = args(0)
    val conf = new SparkConf().setAppName("PageRank Application")
    val sc = new SparkContext(conf)

    val lines = sc.textFile(input).flatMap(file => file.split("\n"))

    val adjList = lines.map { line =>
      val columns = line.split("\t")
      val xml = Jsoup.parse(columns(3))
      val target = xml.getElementsByTag("target")
      var res = new ListBuffer[String]()
      for (t <- 0 to target.size() - 1) {
        res += target.get(t).text()
      }
      (columns(1), res.toList)
    }.cache()

    var ranks = adjList.mapValues(v => 1.0)

    val iterations = args(2).toInt

    for (i <- 1 to iterations) {
      val contribs = adjList.join(ranks).values.flatMap {
        case (urls, rank) =>
          val size = urls.size
          urls.map(url => (url, rank / size))
      }
      ranks = contribs.reduceByKey(_ + _).mapValues(0.15 + 0.85 * _)
    }
    val output = ranks.map(item => item.swap).sortByKey(false, 1).take(100)
    val result = sc.parallelize(output)
    result.coalesce(1).saveAsTextFile(args(1))
    sc.stop()
  }
}
