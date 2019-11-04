package cli

import org.scalatest.{BeforeAndAfterAll, FlatSpec}


class StateManagerSpec extends FlatSpec with BeforeAndAfterAll{

  val stateManager = StateManager("testsparkcli")

  "stateManager" should "add a cluster" in {
    stateManager.addCluster("test123", "testcluster")
    val state =stateManager.getClusters()
    assert(state.head.clusterId == "test123" && state.head.clusterName == "testcluster")
  }

  it should "remove clusters" in {
    stateManager.addCluster("test2", "testcluster2")
    stateManager.removeCluster("testcluster2")
    val state = stateManager.getClusters()
    assert(state.length == 1)
  }

  it should "delete all clusters" in {
    stateManager.removeAll()
    val state = stateManager.getClusters()
    assert(state.isEmpty)
  }

  override def afterAll(): Unit = {
    super.afterAll()
    stateManager.removeAll()
  }
}
