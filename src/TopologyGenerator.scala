import scala.io.Source
import scala.util.Random
import scala.collection.mutable._
import java.nio.file._
import java.io.BufferedWriter

/**
  * Created by pxsalehi on 31.01.17.
  */
object TopologyGenerator {
  val latencyFilename = "./pw-1715-latencies"
  val twitterFilename = "datasets/twitter_follows.txt"
  val noOfNodes = 1715
  val infinity = Int.MaxValue
  val throughputMin = 120
  val throughputMax = 300
  
  implicit def stringTuple3ToIntTuple3(t: (String, String, String)): (Int, Int, Int) = 
    (t._1.toInt, t._2.toInt, t._3.toInt)
  
  def main(args: Array[String]): Unit = {
//    val sizes = Array(10, 20, 50, 100, 200, 300, 400, 500, 600, 700, 800, 900, 1000)
//    sizes.foreach(generateTopology(_))
    
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
  
  private def generateTopology(size: Int) {
    println(s"Generating topology of size $size")
    // create adjacency table
    val all = Array.tabulate(noOfNodes, noOfNodes)((i, j) => -1)
    for(line <- Source.fromFile(latencyFilename).getLines()) {
      val toks = line.split(" ").map(t => t.toInt)
      if (toks.length < 3)
        println("*** Warning: invalid line with less than three tokens! " + line)
      else
        all(toks(0))(toks(1)) = toks(2)/2  // file records are RTT
    }
    // make sure adj table is symmetrical
    for(i <- 0 until noOfNodes)
      for(j <- 0 until noOfNodes)
        all(i)(j) match {
        case x if x <= 0 => {
          // then (j, i) should be -1 too, if not set (i, j) to (j, i)
          all(i)(j) = infinity
          all(j)(i) = infinity
        }
        case x if x > 0 => {
          // (j, i) should be defined, if not use same value
          if (all(j)(i) == -1)
            all(j)(i) = all(i)(j)
        }
//        case 0 => {
//          // consider sub millisecond latencies as random between 1 to 10 to 
//          // avoid a spanning tree where most edges are 1 
//          val lat = -1 //Random.nextInt(10) + 1 
//          all(i)(j) = lat
//          all(j)(i) = lat
//        }
    }
    for (i <- 0 until noOfNodes)
      all(i)(i) = 0
    // select a subset of the dataset
    val network = Set[Int]()
    val queue = Queue[Int]()
    var root = -1
    do {
      // take a random node as root
      root = Random.nextInt(noOfNodes)
      // do a BFS traversal until a graph of required size
      network.clear
      queue.clear
      queue += root
      do {
        val node = queue.dequeue
        network += node
        queue.enqueue(getChildren(node, all)
            //.filter(c => all(c).filter(i => i != infinity).size > 1) 
            .toSeq: _*)
      } while (network.size < size && queue.nonEmpty)
    } while(network.size < size)
    assert(network.size == size)
    println(root)
    println(network)
    // generate an adj matrix for the selected graph
    // create a map from nodes to their new ids which is in the range [0, size)
    // since root is zero, remove root and later add to beginning
    network -= root
    val idMap = (root::network.toList.diff(List(root))).zipWithIndex  // generates (old_id, new_id)
                    .map(t => t._2 -> t._1).toMap  // creates a map (new_id, old_id)
    val adj = Array.tabulate(size, size)( (i, j) => all(idMap(i))(idMap(j)) )
    val networkRoot = 0
    adj.deep.mkString.split("Array").foreach(println)
    // find shortest path tree from root
    val pathNodes = Set(networkRoot)
    val pathEdges = Set[(Int, Int)]()
    val closestNode = Map[Int, Int]()
    val length = new Array[Int](size)
    val vertices = idMap.keys
    vertices.filter(v => !pathNodes.contains(v)).foreach(v => {
      closestNode(v) = networkRoot
      length(v) = adj(networkRoot)(v)
    })
    for(i <- 0 to size-2) {
      // find vertex to add to path with minimum len
      var min = infinity
      var near = -1
      for(v <- vertices.filter(v => v != networkRoot)) {
        if (length(v) >= 0 && length(v) < min) {
          min = length(v)
          near = v
        }
      }
      assert(near != -1)
      pathEdges += ((closestNode(near), near))
      // updates shortest distance of the remaining nodes
      for(v <- vertices.filter(v => v != networkRoot)) {
        if (length(near) + adj(near)(v) < length(v)) {
          length(v) = length(near) + adj(near)(v)
          closestNode(v) = near
        }
        // add near to pathnodes
        pathNodes += near
        length(near) = -1
      }
    }
    assert(pathEdges.size == size - 1)
    pathEdges.foreach(t => println(s"$t: ${adj(t._1)(t._2)}"))
    // TODO: check tree is connected
    // generate non-existing latencies using random selection among latencies not existing in the tree
    // write to file
    val degrees = new Array[Int](size)
    pathEdges.foreach(e => {degrees(e._1) += 1; degrees(e._2) += 1})
    val throughputs = Array.fill(size)(Random.nextInt(throughputMax - throughputMin) + throughputMin)
    val writer = Files.newBufferedWriter(Paths.get("topology" + size))
    writer.write(s"No of nodes:$size\n")
    writer.write(s"No of edges:${pathEdges.size}\n\nNodes:\n")
    for (v <- vertices.toList.sorted)
      writer.write(s"$v\t${degrees(v)}\t${throughputs(v)}\n")
    writer.write("\nEdges:\n")
    for((e, i) <- pathEdges.toList.sortBy(_._1).zipWithIndex)
      writer.write(s"$i\t${e._1}\t${e._2}\t${adj(e._1)(e._2)}\n")
    writer.close()
  }
  
  private def getChildren(node: Int, adj: Array[Array[Int]]): collection.immutable.Set[Int] = {
    adj(node).zipWithIndex.filter(t => t._1 != infinity).map(t => t._2).toSet
  }
  
}
