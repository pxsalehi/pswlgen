package topology

/**
  * Created by pxsalehi on 31.03.17.
  */
object ConfigKeys {
  val TOPO_GEN_SECTION = "topogen"

  val DATASET_KEY = "dataset"
  val MIN_THROUGHPUT_KEY = "min_throughput"
  val MAX_THROUGHPUT_KEY = "max_throughput"
  val SEED_KEY = "seed"
  val TOPOLOGY_SIZE_KEY = "topology_size"

  val DATASET = TOPO_GEN_SECTION + "." + DATASET_KEY
  val MIN_THROUGHPUT = TOPO_GEN_SECTION + "." + MIN_THROUGHPUT_KEY
  val MAX_THROUGHPUT = TOPO_GEN_SECTION + "." + MAX_THROUGHPUT_KEY
  val SEED = TOPO_GEN_SECTION + "." + SEED_KEY
  val TOPOLOGY_SIZE = TOPO_GEN_SECTION + "." + TOPOLOGY_SIZE_KEY
}
