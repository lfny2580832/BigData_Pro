package bigdata.zookeeper;

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.Watcher.Event;
import org.apache.zookeeper.data.Stat;

import java.io.FileOutputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.concurrent.CountDownLatch;

public class zkClient {
	private static final String connectString = "hadoop01:2181,hadoop02:2181,hadoop03:2181";
	private static final int sessionTimeout = 5000;
	private static ZooKeeper zk = null;
	private static CountDownLatch countDownLatch = new CountDownLatch(1);
	private static final String rootNode = "/servers";
	private static List<String> runningServerList = null; // 正在运行的服务器列表
	private static String connectedServer;// 客户端已经连接的服务器地址
	private static int clientNumber;// 客户端编号
	private static Stat stat = new Stat();
	private static final String localPath = "/Users/niuyan/jdbc.txt";

	public void getConn() throws IOException {
		zk = new ZooKeeper(connectString, sessionTimeout, new Watcher() {
			@Override
			public void process(WatchedEvent event) {
				System.out.println("收到监听通知！");
				System.out.println("type:" + event.getType() + ",path:" + event.getPath());

				if (event.getState() == Event.KeeperState.SyncConnected) {
					// 注意：客户端向服务端注册的Watcher是一次性的，一旦服务端向客户端发送一次通知后，这个Watcher失效，
					try {
						if (event.getType() == Event.EventType.None && event.getPath() == null) {
							System.out.println("Zookeeper客户端与服务端成功建立连接！");
							countDownLatch.countDown();
						} else if (event.getType() == Event.EventType.NodeDataChanged) {
							System.out.println("通知:" + event.getPath() + "节点数据发生变化");
							zkClient.getZnodeData(event.getPath(), true);
						}
					} catch (KeeperException | InterruptedException | IOException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
				}

			}
		});

	}

	public void getRunningServers(String path) throws KeeperException, InterruptedException {
		runningServerList = zk.getChildren(path, true);
		System.out.println("Running servers:" + runningServerList);
	}

	public String getTargetServerAddress(int clientNumber) {
		if (runningServerList != null && runningServerList.size() > 0) {
			System.out.println("clientNumber:" + clientNumber);
			System.out.println("running servers:" + runningServerList.size());
			// 算法：targetServer = clientNumber % runningServerlist.size
			int serverIndex = clientNumber % runningServerList.size(); // 目标服务器在正在运行的服务器列表中的位置
			return runningServerList.get(serverIndex);// 获取目标服务器的访问地址
		} else {
			return null;
		}
	}

	public int getClientNumber() {
		Random random = new Random();
		return random.nextInt(1000);
	}

	public boolean ifConnected() {
		if (runningServerList != null && runningServerList.size() > 0 && connectedServer != null) {
			for (String server : runningServerList) {
				if (connectedServer.equals(server))
					return true;
			}
		}
		return false;
	}

	// byte[]写入文件
	public static void createFile(String path, byte[] content) throws IOException {
		FileOutputStream fos = new FileOutputStream(path);
		fos.write(content);
		fos.close();
	}

	// 获取节点数据，并写入本地jdbc.txt文件
	public static void getZnodeData(String path, boolean watch)
			throws KeeperException, InterruptedException, IOException {
		if (zk != null) {
			byte[] data = zk.getData("/jdbc", true, stat);
			zkClient.createFile(localPath, data);
		}
	}

	public void work(String serverUrl) throws InterruptedException, KeeperException, IOException {
		System.out.println("serverUrl:" + serverUrl);
		System.out.println("client working ...");
		connectedServer = serverUrl;
		// 从Zookeeper集群“/jdbc”节点读取数据，并将数据保存到本地，格式同jdbc.txt相同
		zkClient.getZnodeData("/jdbc", true);

		if (!ifConnected()) {
			System.out.println(connectedServer + "连接失效，重新获取服务器地址！");
			connectedServer = getTargetServerAddress(clientNumber);
			System.out.println("重新连接：" + connectedServer + "成功！");
		}
		Thread.sleep(Long.MAX_VALUE);
	}

	public static void main(String[] args) throws KeeperException, IOException {
		// 获取zk连接
		zkClient client = new zkClient();
		try {
			client.getConn();
			countDownLatch.await();
		} catch (IOException e) {
			e.printStackTrace();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}

		// 从zk获取正在运行的服务器列表
		try {
			client.getRunningServers(rootNode);
		} catch (KeeperException e) {
			e.printStackTrace();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}

		// 获取客户端要访问的服务器地址
		clientNumber = client.getClientNumber();
		String targetUrl = client.getTargetServerAddress(clientNumber);

		// 执行具体的业务功能
		try {
			client.work(targetUrl);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}
}
