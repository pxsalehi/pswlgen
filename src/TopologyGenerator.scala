import scala.io.Source
import scala.util.Random
import scala.collection.mutable._
import java.nio.file._
import java.io.BufferedWriter

/**
  * Created by pxsalehi on 31.01.17.
  */
object TopologyGenerator {
  val latencyFilename = "datasets/peerwise-latencies.txt"
  val noOfNodes = 1715
  val INFINITY = Int.MaxValue
  val throughputMin = 120
  val throughputMax = 300
  type Edge = (Int, Int)
  
  case class Subgraph(root: Int, adj: Array[Array[Int]]) {
    def size = adj.size
    def vertices = 0.until(size).toSet
    def apply(x: Int) = adj(x)
  }
  
  def main(args: Array[String]): Unit = {
    val sizes = Array(10, 20, 50, 100, 200, 300, 400, 500, 600, 700, 800, 900, 1000)
    val seed = 276980650
    Random.setSeed(seed)
    println("Reading dataset...")
    val datasetGraph = readGraph(latencyFilename, noOfNodes)
    sizes.foreach(generateTopology(datasetGraph, _))
    println("All done!")
  }
  
  private def generateTopology(datasetGraph: Array[Array[Int]], size: Int) {
    println(s"Generating topology of size $size")
    // create adjacency table
    println("Extracting subgraph...")
    val graph = extractSubgraph(datasetGraph, size)
    //graph.adj.deep.mkString.split("Array").foreach(l => println(l.replace(INFINITY.toString, "X")))
    println("Calculating shortest path tree...")
    val pathEdges = findShortestPathTree(graph)
    // generate non-existing latencies using random selection among latencies not existing in the tree
    //
    // write to file
    val degrees = new Array[Int](size)
    pathEdges.foreach(e => {degrees(e._1) += 1; degrees(e._2) += 1})
    val throughputs = Array.fill(size)(Random.nextInt(throughputMax - throughputMin) + throughputMin)
    val writer = Files.newBufferedWriter(Paths.get("topology" + size))
    writer.write(s"No of nodes:$size\n")
    writer.write(s"No of edges:${pathEdges.size}\n\nNodes:\n")
    for (v <- graph.vertices.toList.sorted)
      writer.write(s"$v\t${degrees(v)}\t${throughputs(v)}\n")
    writer.write("\nEdges:\n")
    for((e, i) <- pathEdges.toList.sortBy(_._1).zipWithIndex)
      writer.write(s"$i\t${e._1}\t${e._2}\t${graph(e._1)(e._2)}\n")
    writer.close()    
  }
  
  private def readGraph(filename: String, noOfNodes: Int): Array[Array[Int]] = {
    val all = Array.tabulate(noOfNodes, noOfNodes)((i, j) => INFINITY)
    for(line <- Source.fromFile(latencyFilename).getLines()) {
      val toks = line.split(" ").map(t => t.toInt)
      if (toks.length < 3)
        println("*** Warning: invalid line with less than three tokens! " + line)
      else if(toks(2) >= 1)
        all(toks(0))(toks(1)) = if (toks(2) == 1) 1 else toks(2)/2  // file records are RTT
    }
    for (i <- 0 until noOfNodes)
      all(i)(i) = 0
    // make sure adj table is symmetrical
    for(i <- 0 until noOfNodes)
      for(j <- 0 until noOfNodes)
        if(i != j && all(i)(j) == INFINITY) {
          // make sure (j, i) also doesn't exist, otherwise use same value
            if(all(j)(i) < INFINITY)
              all(i)(j) = all(j)(i)
        }
    return all
  }
  
  private def extractSubgraph(graph: Array[Array[Int]], subgraphSize: Int): Subgraph = {
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
        val children = getChildren(node, graph).filter(c => !network.contains(c))
        val nonLocalChildren = children.filter(c => graph(node)(c) > 1)
        if (nonLocalChildren.isEmpty) {
          queue.enqueue(children.toSeq: _*)
          println("***** Warning all neighbors are local!")
        } else
          queue.enqueue(children.toSeq: _*)
      } while (network.size < subgraphSize && queue.nonEmpty)
    } while(network.size < subgraphSize)
    assert (network.size == subgraphSize)
    // generate an adj matrix for the selected graph
    // create a map from nodes to their new ids which is in the range [0, size)
    // since root is zero, remove root and later add to beginning
    network -= root
    val idMap = (root::network.toList.diff(List(root))).zipWithIndex  // generates (old_id, new_id)
                    .map(t => t._2 -> t._1).toMap  // creates a map (new_id, old_id)
    val adj = Array.tabulate(subgraphSize, subgraphSize)( (i, j) => graph(idMap(i))(idMap(j)) )
    root = 0
    return new Subgraph(root, adj)
  }
  
  private def findShortestPathTree(graph: Subgraph): collection.immutable.Set[Edge] = {
    // find shortest path tree from root
    val parent = Array.fill(graph.size)(-1)
    val distance = Array.fill(graph.size)(INFINITY)
    val visited = Set[Int]()
    distance(graph.root) = 0
    for(i <- 0 until graph.size) {
      // find vertex to add to path with minimum len
      var min = INFINITY
      var near = -1
      for(v <- graph.vertices.diff(visited)) {
        if (distance(v) >= 0 && distance(v) < min) {
          min = distance(v)
          near = v
        }
      }
      assert(near != -1)
      visited += near
      for(neighbor <- getChildren(near, graph.adj))
        if (distance(near) + graph(near)(neighbor) < distance(neighbor)) {
          distance(neighbor) = distance(near) + graph(near)(neighbor)
          parent(neighbor) = near
        }
    }
    // extract the tree
//    val pathEdges = Set[Edge]()
//    for(v <- (1 until graph.size))
//      pathEdges ++= generateEdges(parent, v)
    val pathEdges = (0 until graph.size).flatMap(v => generateEdges(parent, v)).toSet
    assert(pathEdges.size == graph.size - 1)
    // TODO: check tree is connected
    return pathEdges.toSet
  }
  
  // generate edges connecting vertex v to root
  private def generateEdges(parent: Array[Int], v: Int): Set[Edge] = {
    var p = parent(v)
    var c = v
    val pathes = Set[Edge]()
    while(p != -1) {
      pathes += ((c, p))
      c = p
      p = parent(p)
    }
    return pathes
  }
  
  private def getChildren(node: Int, adj: Array[Array[Int]]): collection.immutable.Set[Int] = {
    adj(node).zipWithIndex.filter(t => t._1 != INFINITY).map(t => t._2).toSet
  }
  
}
