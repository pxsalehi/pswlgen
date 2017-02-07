import java.nio.file.{Files, Paths}

import scala.io.Source

/**
  * Created by pxsalehi on 2/5/2017.
  */
object ExtractPopularity {
  val twitterFilename = "datasets/twitter_follows.txt"

  def main(args: Array[String]): Unit = {
    if (args.length < 2) {
      println("Invalid argument!\nExtractPopularity no_of_publishers output_file")
      sys.exit()
    }
    val noOfPublishers = args(0).toInt
    val outputFile = args(1)
    // read list of tuples (a, b) meaning a follows b
    println(s"Reading dataset from $twitterFilename...")
    val tlist = {
      for {line <- Source.fromFile(twitterFilename).getLines()
           tokens = line.split("\\s+")
           tuple = (tokens(0), tokens(1))} yield tuple
    }.toList
    val users = tlist.flatMap(t => List(t._1, t._2)).toSet
    val publishers = tlist.map(t => t._2).toSet
    val subscribers = tlist.map(t => t._1).toSet
    println("Total client count: " + users.size)
    println("subscribers: " + subscribers.size)
    println("publishers: " + publishers.size)
    val subCountPerPublisher = tlist.groupBy(t => t._2).map(t => (t._1, t._2.size))
    val minSubCount = subCountPerPublisher.values.min
    val maxSubCount = subCountPerPublisher.values.max
    //subCountPerPublisher.toList.sortBy(_._2).reverse.foreach(println)
    assert (subCountPerPublisher.keys.size == publishers.size)
    println("Min sub count " + minSubCount)
    println("Max sub count " + maxSubCount)
    // given each pair of (pub, sub) sizes, generate a file that shows number of subs for each publishers
    println(s"Extracting popularities for $noOfPublishers publishers...")
    val wloadSubCounts = {
      val pubSubset = subCountPerPublisher.take(noOfPublishers)
      val difference = pubSubset.last._2 - 1
      pubSubset.map(t => (t._1, math.max(t._2 - difference, 1)))
    }
    // transform the sub counts to percentage of total subs
    val totalSubs: Double = wloadSubCounts.values.sum
    val wloadSubPercentage = wloadSubCounts.map(t => (t._1, t._2 / totalSubs))
    val wloadToWrite = wloadSubPercentage.toList.sortBy(_._2).reverse.zipWithIndex
        .map({case ((pub, subCount), index) => (index, subCount)})
    val writer = Files.newBufferedWriter(Paths.get(outputFile))
    for((publisher, popularity) <- wloadToWrite) writer.write(s"$publisher $popularity\n")
    writer.close()
    println("Done!")
  }
}
