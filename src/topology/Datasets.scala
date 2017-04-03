package topology

/**
  * Created by pxsalehi on 03.04.17.
  */

case class DatasetInfo(filename: String, noOfNodes: Int)

object Datasets {
  val PEERWISE = "peerwise"
  val PLANETLAB = "planetlab"

  val infos = Map(
    PEERWISE -> DatasetInfo("datasets/peerwise-latencies.txt", 1715),
    PLANETLAB -> DatasetInfo("datasets/planetlab-latencies.txt", 325)
  )

  def getDatasetFilename(dataset: String): Option[String] =
    if (!infos.contains(dataset)) None else Some(infos(dataset).filename)

  def getDatasetNoOfNodes(dataset: String): Option[Int] =
    if (!infos.contains(dataset)) None else Some(infos(dataset).noOfNodes)
}
