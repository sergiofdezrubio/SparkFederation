package SparkFederation.ServerFed.zkCoordinatorFed

import java.util.concurrent.CountDownLatch

import org.apache.zookeeper.Watcher.Event
import org.apache.zookeeper._
import org.apache.zookeeper.data.Stat



class zkExecutor () {

  // https://www.tutorialspoint.com/zookeeper/zookeeper_api.htm

  @throws(classOf[KeeperException])
  @throws(classOf[InterruptedException])
  def createZnode (path: String, data: Array[Byte] ): Unit = {
    val zkSession = new ZkConnection().createZkSession()
    zkSession.create(path, data, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT)
    zkSession.close()
  }

  @throws(classOf[KeeperException])
  @throws(classOf[InterruptedException])
  def existsZnode (path: String) : Option[Stat]  = {
    val zkSession = new ZkConnection().createZkSession()
    val flag = Option[Stat](zkSession.exists(path, true))
    zkSession.close()
    flag
  }

  @throws(classOf[KeeperException])
  @throws(classOf[InterruptedException])
  def getData(path : String ) : Option[Array[Byte]] = {

    val zkSession = new ZkConnection().createZkSession()
    val znode = this.existsZnode(path)
    val connectedSignal = new CountDownLatch(1)

    if (! znode.isDefined ) {
      return Option.empty[Array[Byte]]

    }

    val data = zkSession.getData(path, new Watcher {

         override def process(we: WatchedEvent): Unit = {
           if (we.getType == Event.EventType.None) we.getState match {
             case Event.KeeperState.Expired =>
               connectedSignal.countDown()
           }
         }
      }, null
    )
    zkSession.close()
    Option(data)
  }

  @throws(classOf[KeeperException])
  @throws(classOf[InterruptedException])
  def setData (path : String , data: Array[Byte] ): Unit = {
    val zkSession = new ZkConnection().createZkSession()
    zkSession.setData(path, data, zkSession.exists(path, true).getVersion)
    zkSession.close()
  }

  @throws(classOf[KeeperException])
  @throws(classOf[InterruptedException])
  def getChildern (path : String ) : Option[Array[String]]  = {

    val zkSession = new ZkConnection().createZkSession()
    val znode = this.existsZnode(path)
    if ( ! znode.isDefined) {
      return Option.empty[Array[String]]
    }
    val children = zkSession.getChildren(path,false)
    Option[Array[String]](children.toArray(Array.ofDim[String](children.size())))
  }

  @throws(classOf[KeeperException])
  @throws(classOf[InterruptedException])
  def deleteZnode (path : String ): Unit = {
    val zkSession = new ZkConnection().createZkSession()
    zkSession.delete(path,zkSession.exists(path,true).getVersion())
  }




}
