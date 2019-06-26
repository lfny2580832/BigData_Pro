package bigdata.zookeeper;

import java.io.IOException;
import java.util.concurrent.CountDownLatch;

import org.apache.log4j.chainsaw.Main;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.Watcher.Event;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.data.Stat;

public class DistributedServer {

	private static String connectString = "hadoop01:2181,hadoop02:2181,hadoop03:2181";
	private static int sessionTimeout = 5000;
	private ZooKeeper zk = null; 
	private static CountDownLatch countDownLatch = new CountDownLatch(1);
	private static final String rootNode = "/servers";
	
	public  void getConn() throws IOException {
		
		zk = new ZooKeeper(connectString, sessionTimeout, new Watcher() {
			@Override
			public void process(WatchedEvent event) {

				System.out.println("收到监听通知");
				System.out.println("type:" + event.getType() + ",path:" + event.getPath());
				if(event.getState() == Event.KeeperState.SyncConnected) {
					//客户端向服务端注册的watcher是一次性的，客户端需要反复注册watcher
					//服务端向客户端发送成功建立连接的通知
					if(event.getType() == Event.EventType.None && event.getPath() == null) {
						System.out.println("Zookeeper客户端与服务端成功建立连接");
						countDownLatch.countDown();
					}

				}
			}
		});
		
	}
	
	//服务端向zk注册服务器信息
	public void registerServer(String serverNodeName) throws KeeperException, InterruptedException {
		String serverNodePath = rootNode + "/" + serverNodeName;
		String rs = zk.create(serverNodePath, serverNodeName.getBytes(), Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
		if(rs != null) {
			System.out.println(serverNodeName + "节点已上线");
		}
	}
	
	//执行具体的业务功能
	public void work() {
		System.out.println("server working...");
		try {
			Thread.sleep(Long.MAX_VALUE);//模拟长期运行
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}
	
	public static void main(String[] args) throws Exception {
		//获取zookeeper连接
		DistributedServer server = new DistributedServer();
		server.getConn();
		countDownLatch.await();
		
		String hostname = args.length == 0 ? "localhost" : args[0];
		
		//向zk注册server信息
		server.registerServer(hostname);
		
		//执行具体业务功能
		server.work();
	}
}
