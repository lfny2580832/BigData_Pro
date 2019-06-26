package bigdata.zookeeper;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.Watcher.Event.EventType;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.data.Stat;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.CountDownLatch;

public class ZookeeperClient {
	private static String connectString = "hadoop01:2181,hadoop02:2181,hadoop03:2181";
	private static int sessionTimeout = 5000;
	private static ZooKeeper zk = null;
	private static CountDownLatch countDownLatch = new CountDownLatch(1);
	private static Stat stat = new Stat();
	public static void getConn() throws IOException {
		
		zk = new ZooKeeper(connectString, sessionTimeout, new Watcher() {
			@Override
			public void process(WatchedEvent event) {
				//作用：客户端接收服务端发送的监听通知
				System.out.println("收到监听通知");
				System.out.println("type:" + event.getType() + ",path:" + event.getPath());
				if(event.getState() == Event.KeeperState.SyncConnected) {
					//客户端向服务端注册的watcher是一次性的，客户端需要反复注册watcher
					//服务端向客户端发送成功建立连接的通知
					try {
						if(event.getType() == Event.EventType.None && event.getPath() == null) {
							System.out.println("Zookeeper客户端与服务端成功建立连接");
							countDownLatch.countDown();
						}else if(event.getType() == Event.EventType.NodeChildrenChanged) {
							System.out.println("通知:" + event.getPath() + "节点的子节点发生变化");
							ZookeeperClient.getChildrenNodes(event.getPath(), true);
						}else if(event.getType() == Event.EventType.NodeDataChanged) {
							System.out.println("通知:" + event.getPath() + "节点数据发生变化");
							ZookeeperClient.getZnodeData(event.getPath(), true);	//重新注册监听事件
						}else if(event.getType() == Event.EventType.NodeDeleted) {
							System.out.println("通知:" + event.getPath() + "节点被删除");
							ZookeeperClient.existsZnode(event.getPath(), true);	//重新注册监听事件
						}else if(event.getType() == Event.EventType.NodeCreated) {
							System.out.println("通知:" + event.getPath() + "节点被创建");
							ZookeeperClient.existsZnode(event.getPath(), true);	//重新注册监听事件
						}
					} catch (KeeperException e) {
						e.printStackTrace();
					} catch (InterruptedException e) {
						e.printStackTrace();
					}
				}
			}
		});
		
	}
	
	//创建节点
	public static void createZnode(String path, byte[] data, CreateMode createMode) throws KeeperException, InterruptedException {
		if(zk != null) {
			//String path：znode路径
			//byte data[]：znode值
			//List<ACL> acl：znode访问权限
            //CreateMode createMode：节点类型
			zk.create(path, data, ZooDefs.Ids.OPEN_ACL_UNSAFE, createMode);
		}
	}
	
	//获取节点所有子节点
	public static void getChildrenNodes(String path, boolean watch) throws KeeperException, InterruptedException {
		if(zk != null) {
			List<String> children = zk.getChildren(path, watch);
			System.out.println(path + "节点的所有字节点:" + children);;
		}
	}
	
	//获取节点数据
	public static void getZnodeData(String path, boolean watch) throws KeeperException, InterruptedException {
		if(zk != null) {
			byte[] data = zk.getData(path, watch, stat);
			System.out.println("获取到的" + path + "节点数据" + new String(data));
			System.out.println("czxid:" + stat.getCzxid() + "mzxid" + stat.getMzxid());

		}
	}
	
	//设置节点数据
	public static Stat setZnodeData(String path, byte[] data,int version) throws KeeperException, InterruptedException {
		Stat stat1 = null;
		if(zk != null) {
			//version 更新的数据版本号，修改一次自动+1，-1代表最新版本
			stat1 =  zk.setData(path, data, version);
			System.out.println(path + "节点信息：czxid = " + stat1.getCzxid() + ",mzxid=" 
					+ stat1.getMzxid() + ",dataVersion:" + stat1.getVersion());
		}
		return stat1;
	}
	
	//删除节点
	public static void deleteZnode(String path, int version) throws InterruptedException, KeeperException {
		if(zk != null) {
			//只允许删除叶子结点
			zk.delete(path, version);
			System.out.println("已删除" + path + "结点");
		}
	}
	
	//检查节点是否存在
	public static void existsZnode(String path, boolean watch) throws KeeperException, InterruptedException {
		if(zk != null) {
			Stat statrs = zk.exists(path, watch);
			if(statrs == null) {
				System.out.println(path + "节点不存在");
			}
		}
	}
	
	public static void main(String[] args) {
		try {
			ZookeeperClient.getConn();
			System.out.println("当前连接状态：" + zk.getState());
			countDownLatch.await();	//等待服务端向客户端发送成功建立连接的通知
			
			//创建znode节点,同名会异常，不能创建包含子节点的嵌套节点
//			String path = "/zk-test1";
//			ZookeeperClient.createZnode(path, "hello zktest".getBytes(), CreateMode.PERSISTENT);
//			ZookeeperClient.createZnode("/zk-test1/tmp1", "hello tmp1".getBytes(), CreateMode.PERSISTENT);

			//获取节点下所有子节点
//			ZookeeperClient.getChildrenNodes("/zk-test1", true);

			//获取节点数据
//			ZookeeperClient.getZnodeData("/zk-test1", true);
			
			//更新节点数据
//			Stat stat1 = ZookeeperClient.setZnodeData("/zk-test1", "111".getBytes(), -1);
//			Stat stat2 = ZookeeperClient.setZnodeData("/zk-test1", "222".getBytes(), stat1.getVersion());
//			Stat stat3 = ZookeeperClient.setZnodeData("/zk-test1", "333".getBytes(), stat2.getVersion());

			//删除节点，只能删除叶子结点
//			ZookeeperClient.deleteZnode("/zk-test1", -1);
			
			//检测节点是否存在
			ZookeeperClient.existsZnode("/zk-test4", true);
			
			Thread.sleep(Integer.MAX_VALUE);
			System.out.println("客户端运行结束！");
		} catch (IOException e) {
			e.printStackTrace();
		} catch (InterruptedException e) {
			e.printStackTrace();
		} catch (KeeperException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	}
}