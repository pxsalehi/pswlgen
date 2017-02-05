import scala.io.Source

/**
  * Created by pxsalehi on 2/5/2017.
  */
object ExtractPopularity {
  val twitterFilename = "datasets/twitter_follows.txt"

  def main(args: Array[String]): Unit = {
    // read list of tuples (a, b) meaning a follows b
    val tlist = Source.fromFile(twitterFilename).getLines().map(l => l.split("\\s+"))
      .map(arr => (arr(0), arr(1))).toList
    val users = tlist.flatMap(t => List(t._1, t._2)).toSet
    val publishers = tlist.map(t => t._2).toSet
    val subscribers = tlist.map(t => t._1).toSet
    println("all users: " + users.size)
    println("subscribers: " + subscribers.size)
    println("publishers: " + publishers.size)
    val subCountPerPublisher = tlist.groupBy(t => t._2).map(t => (t._1, t._2.size))
    val minSubCount = subCountPerPublisher.values.min
    val maxSubCount = subCountPerPublisher.values.max
    subCountPerPublisher.toList.sortBy(_._2).reverse.foreach(println)
    assert (subCountPerPublisher.keys.size == publishers.size)
    println("Min sub count " + minSubCount)
    println("Max sub count " + maxSubCount)
    val noOfPublishers = 100
    //    val subSizeList = List(2000, 5000, 10000, 20000, 50000, 100000, 200000, 500000)
    // given each pair of (pub, sub) sizes, generate a file that shows number of subs for each publishers
    var wloadSubCounts = subCountPerPublisher.take(noOfPublishers)
    val difference = wloadSubCounts.last._2 - 1
    wloadSubCounts = wloadSubCounts.map(t => (t._1, math.max(t._2 - difference, 1)))
    wloadSubCounts.toList.sortBy(_._2).reverse.foreach(println)
    // transform the sub counts to percentage of total subs
    val totalSubs: Double = wloadSubCounts.map(t => t._2).reduce(_+_)
    val wloadSubPercentage = wloadSubCounts.map(t => (t._1, t._2 / totalSubs))
    println("Popularities:")
    wloadSubPercentage.toList.sortBy(_._2).reverse.zipWithIndex.map(t => (t._2, t._1._2)).foreach(println)
  }
}
