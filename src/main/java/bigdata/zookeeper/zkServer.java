package bigdata.zookeeper;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.concurrent.CountDownLatch;
import org.apache.log4j.chainsaw.Main;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.Watcher.Event;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.data.Stat;

public class zkServer {
	private static String connectString = "hadoop01:2181,hadoop02:2181,hadoop03:2181";
	private static int sessionTimeout = 5000;
	private static ZooKeeper zk = null;
	private static CountDownLatch countDownLatch = new CountDownLatch(1);
	private static final String rootNode = "/servers";
	private static Stat stat = new Stat();
    private static final String localPath = "/Users/niuyan/jdbc.txt";

	public void getConn() throws IOException {

		zk = new ZooKeeper(connectString, sessionTimeout, new Watcher() {
			@Override
			public void process(WatchedEvent event) {

				System.out.println("收到监听通知");
				System.out.println("type:" + event.getType() + ",path:" + event.getPath());
				if (event.getState() == Event.KeeperState.SyncConnected) {
					// 客户端向服务端注册的watcher是一次性的，客户端需要反复注册watcher
					// 服务端向客户端发送成功建立连接的通知
					try {
						if (event.getType() == Event.EventType.None && event.getPath() == null) {
							System.out.println("Zookeeper客户端与服务端成功建立连接");
							countDownLatch.countDown();
						}else if (event.getType() == Event.EventType.NodeCreated) {
							System.out.println("通知:" + event.getPath() + "节点被创建");
							zkServer.existsZnode(event.getPath(), true);
							// 重新注册监听事件
						}
					} catch (KeeperException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					} catch (InterruptedException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}

				}
			}
		});
	}

	// 服务端向zk注册服务器信息
	public void registerServer(String serverNodeName) throws KeeperException, InterruptedException {
		String serverNodePath = rootNode + "/" + serverNodeName;
		String rs = zk.create(serverNodePath, serverNodeName.getBytes(), Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
		if (rs != null) {
			System.out.println(serverNodeName + "节点已上线");
		}
	}

	// file 转 byte[]
	private static byte[] fileConvertToByteArray(File file) {
		byte[] data = null;
		try {
			FileInputStream fis = new FileInputStream(file);
			ByteArrayOutputStream baos = new ByteArrayOutputStream();
			int len;
			byte[] buffer = new byte[1024];
			while ((len = fis.read(buffer)) != -1) {
				baos.write(buffer, 0, len);
			}
			data = baos.toByteArray();
			fis.close();
			baos.close();
		} catch (Exception e) {
			e.printStackTrace();
		}
		return data;
	}

	// 执行具体的业务功能
	public void work() throws KeeperException, InterruptedException {
		System.out.println("server working...");
		while (true) {
			Thread.sleep(5000);
			// 读取本地jdbc
			File file = new File(localPath);
			byte[] data1 = fileConvertToByteArray(file);
			byte[] data2 = zk.getData("/jdbc", true, stat);
			// 比较jdbc，若数据有变，setNodeData /jdbc
			if(!Arrays.equals(data1, data2)) {
				System.out.println("数据有变，重新设置节点数据");
				zkServer.setZnodeData("/jdbc", data1, -1);
			}else {
				System.out.println("jdbc节点数据不变");
			}
		}
	}

	// 检查节点是否存在
	public static void existsZnode(String path, boolean watch) throws KeeperException, InterruptedException {
		if (zk != null) {
			Stat statrs = zk.exists(path, watch);
			if (statrs == null) {
				System.out.println(path + "节点不存在");
			}
		}
	}

	// 设置节点数据
	public static Stat setZnodeData(String path, byte[] data, int version)
			throws KeeperException, InterruptedException {
		Stat stat1 = null;
		if (zk != null) {
			// version 更新的数据版本号，修改一次自动+1，-1代表最新版本
			stat1 = zk.setData(path, data, version);
			System.out.println(path + "节点信息：czxid = " + stat1.getCzxid() + ",mzxid=" + stat1.getMzxid()
					+ ",dataVersion:" + stat1.getVersion());
		}
		return stat1;
	}

	// 创建znode节点
	public static void createZnode() throws KeeperException, InterruptedException {
		if (zk != null) {
			Stat statrs = zk.exists("/jdbc", true);
			if (statrs == null) {
				System.out.println("节点不存在，创建节点");
				File file = new File(localPath);
				zk.create("/jdbc", fileConvertToByteArray(file), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
			}else {
				System.out.println("节点已创建");
			}
		
		}
	}

	// 获取节点数据
	public static void getZnodeData(String path, boolean watch) throws KeeperException, InterruptedException {
		if (zk != null) {
			byte[] data = zk.getData(path, watch, stat);
			System.out.println("获取到的" + path + "节点数据" + new String(data));
		}
	}

	public static void main(String[] args) throws Exception {
		// 获取zookeeper连接
		zkServer server = new zkServer();
		server.getConn();
		countDownLatch.await();

		String hostname = args.length == 0 ? "localhost" : args[0];

		// 向zk注册server信息
		server.registerServer(hostname);

		// 创建永久Znode节点"/jdbc",并将服务端本地的jdbc.txt文件的数据写入到Znode节点
		zkServer.createZnode();

		// 执行具体业务功能
		server.work();
	}
}
