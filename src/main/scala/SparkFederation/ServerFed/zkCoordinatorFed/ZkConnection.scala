package SparkFederation.ServerFed.zkCoordinatorFed

import java.io.IOException
import java.util.concurrent.CountDownLatch

import SparkFederation.Lib.ZooKeeperProperties
import org.apache.zookeeper.Watcher.Event.KeeperState
import org.apache.zookeeper.{WatchedEvent, Watcher, ZooKeeper}

class ZkConnection {

  // https://www.tutorialspoint.com/zookeeper/zookeeper_api.htm

  var zkConnection: ZooKeeper = null;
  val connectedSignal = new CountDownLatch(1)

  @throws(classOf[IOException])
  @throws(classOf[InterruptedException])
  def createZkSession(): ZooKeeper = {

    val zkSession = new ZooKeeper(ZooKeeperProperties.ZK_HOST, 5000,new Watcher() {

        def process( we : WatchedEvent ) : Unit = {

          if (we.getState() == KeeperState.SyncConnected) {
            connectedSignal.countDown;
          }
        }
      }
    )
    connectedSignal.await
    this.zkConnection = zkSession
    zkSession
  }

  def closeZkSession(): Unit =  this.zkConnection.close()

}
