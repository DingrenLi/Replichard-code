package org.apache.hadoop.zookeeper;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;

import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

public class DistrubutedLock extends ConnectionWatcher {
    public static final Log LOG = LogFactory.getLog(ZooKeeper.class.getName());
    private String root = "/locks";//根
    private String waitNode;//等待前一个锁
    private String myZnode;//当前锁
    private CountDownLatch latch;//计数器
    private int sessionTimeout = 30000;
    private List exception = new ArrayList();


    public String join(String groupPath,Configuration conf)
            throws KeeperException, InterruptedException {

        String path = groupPath + "/"+conf.get("fs.default.name").replaceAll("hdfs://","");
        byte[] a="0".getBytes();
       //建立一个临时节点
        String createdPath = zk.create(path, a,
                ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
        return createdPath;}

       public Boolean join(String groupPath,String data)
                throws KeeperException, InterruptedException {
           String createdPath = zk.create(groupPath, data.getBytes(),
                    ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
            if (createdPath==groupPath) return true;
            else return false;
       }
   /* public void close(){
        try {
            zk.close();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }*/
    public String get_namenode(String path) throws KeeperException, InterruptedException {
        List<String> childList=zk.getChildren(path, false);
        String value=null;
        if (tryLock());
        else waitForLock(waitNode, sessionTimeout);
        value= new String(zk.getData(path+"/"+childList.get(0),false, null)) ;
        int count=Integer.parseInt(value,16);
        String namenode=childList.get(0);
        for (String childName : childList) {
            value= new String(zk.getData(path+"/"+childName, false, null)) ;
            if (Integer.parseInt(value,16)<count){
                count=Integer.parseInt(value,16);
                namenode=childName;
            }}
        byte[] data=Integer.toHexString ((Integer.parseInt(value,16) + 1)).getBytes();
        zk.setData(path + "/" + namenode, data, -1);
        if (zk.getData(path+"/"+namenode,false,null)!=data){
            Thread.sleep(500);
            System.out.println(namenode+":data is invalidate");
        }
        unlock();
        LOG.info("@@@@@@@@@@@@src is:"+path+" "+"namenode is"+namenode+" "+"data is"+new String(data));
        return namenode;

    }
    public boolean tryLock() {
        try {
            String splitStr = "_lock_";
            //创建临时子节点
            myZnode = zk.create(root + "/" + splitStr, new byte[0],
                    ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL);
            System.out.println(myZnode + " is created ");
            //取出所有子节点
            List<String> subNodes = zk.getChildren(root, false);
            //取出所有lockName的锁

            if(myZnode.equals(root+"/"+subNodes.get(0))){
                //如果是最小的节点,则表示取得锁
                return true;}
            //如果不是最小的节点，找到比自己小1的节点
            String subMyZnode = myZnode.substring(myZnode.lastIndexOf("/") + 1);
            int result=Collections.binarySearch(subNodes, subMyZnode);
            if (result<0){
                System.out.println("data is wrong");
            }
            else {
            waitNode = subNodes.get(Collections.binarySearch(subNodes, subMyZnode) - 1);}
        } catch (KeeperException e) {
            throw new LockException(e);
        } catch (InterruptedException e) {
            throw new LockException(e);
        }
        return false;
    }
    private boolean waitForLock(String lower, long waitTime) throws InterruptedException, KeeperException {
        Stat stat = zk.exists(root + "/" + lower, true);
        //判断比自己小一个数的节点是否存在,如果不存在则无需等待锁,同时注册监听
        if(stat != null){
            System.out.println("Thread " + Thread.currentThread().getId() + " waiting for " + root + "/" + lower);
            this.latch = new CountDownLatch(1);
            this.latch.await(waitTime, TimeUnit.MILLISECONDS);
            this.latch = null;
        }
        return true;
    }
    public  void unlock() {
        try {
            System.out.println("unlock " + myZnode);
            zk.delete(myZnode, -1);
            myZnode = null;
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (KeeperException e) {
            e.printStackTrace();
        }
    }
    public class LockException extends RuntimeException {
        private static final long serialVersionUID = 1L;
        public LockException(String e){
            super(e);
        }
        public LockException(Exception e){
            super(e);
        }
    }

public boolean checkState(String groupPath) throws KeeperException, InterruptedException {

    Stat stat2=zk.exists(groupPath,false);


    if (stat2==null){
        return true;
    }
    else return false;

}
    public String get_data(String path) throws KeeperException, InterruptedException {
        String S2=new String(zk.getData(path, false, null));
        return S2;

    }

    public Map get_locks(String src)throws KeeperException,InterruptedException {
       //List<String>  childList=zk.getChildren("/slave16",false);
        Map   locks=new HashMap();
        String value=null;
        if (zk.exists(src, true)!=null){
        value=new String(zk.getData(src,false,null));
            locks.put(src,value);}
        return locks;



        /*for (String childName:childList){
            value=new String(zk.getData("/slave16"+"/"+childName,false,null));
            locks.put("/slave16"+"/"+childName,value);
        }
        childList=zk.getChildren("/slave17",false);
        for (String childName:childList){
            value=new String(zk.getData("/slave17"+"/"+childName,false,null));
            locks.put("/slave17"+"/"+childName,value);
        }
        childList=zk.getChildren("/slave18",false);
        for (String childName:childList){
            value=new String(zk.getData("/slave18"+"/"+childName,false,null));
            locks.put("/slave18"+"/"+childName,value);
        }
        childList=zk.getChildren("/slave19",false);
        for (String childName:childList){
            value=new String(zk.getData("/slave19"+"/"+childName,false,null));
            locks.put("/slave19"+"/"+childName,value);
        }*/

    }

    /**
     * 若本客户端没有得到分布式锁，则进行监听本节点前面的节点（避免羊群效应）
     * @param groupPath
     * @param myName
     * @throws KeeperException
     * @throws InterruptedException
     */
    public void listenNode(final String groupPath, final String myName) throws KeeperException, InterruptedException{

        List<String> childList =  zk.getChildren(groupPath, false);
        String[] myStr = myName.split("-");
        long myId = Long.parseLong(myStr[2]);

        List<Long> idList = new ArrayList<Long>();
        Map<Long, String> sessionMap = new HashMap<Long, String>();

        for (String childName : childList) {
            String[] str = childName.split("-");
            long id = Long.parseLong(str[2]);
            idList.add(id);
            sessionMap.put(id, str[1]+"-"+str[2]);
        }
        Collections.sort(idList);
        int i = idList.indexOf(myId);
        if (i <=0) {
            throw new IllegalArgumentException("数据错误！");
        }

        //得到前面的一个节点
        long headId = idList.get(i-1);

        String headPath = groupPath + "/lock-" + sessionMap.get(headId);
        System.out.println("添加监听：" + headPath);

        Stat stat = zk.exists(headPath, new Watcher(){

            @Override
            public void process(WatchedEvent event) {
                System.out.println("已经触发了" + event.getType() + "事件！");

                try {
                    while(true){
                        if (checkState(groupPath)) {
                            Thread.sleep(3000);
                            System.out.println(new Date() + " 系统关闭！");
                            System.exit(0);
                        }

                        Thread.sleep(3000);
                    }
                } catch (KeeperException e) {
                    e.printStackTrace();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }

            }

        });

        System.out.println(stat);

    }
    public List<String> get_namenode_list() throws InterruptedException,KeeperException{
        List<String> childList=zk.getChildren("/namenode",true);
        return childList;
    }

    //public static void main(String[] args) throws Exception {
      //  DistrubutedLock joinGroup = new DistrubutedLock();
        //joinGroup.connect("localhost:" + "2181");

        //zookeeper的根节点；运行本程序前，需要提前生成
        //String groupName = "zkroot";
        //String memberName = "_locknode_";
        //String path = "/" + groupName + "/" + memberName;

        //String myName = joinGroup.join(path);
      //  if (!joinGroup.checkState(path, myName)) {
        //    joinGroup.listenNode(path, myName);
     //   }

       // Thread.sleep(Integer.MAX_VALUE);

        //joinGroup.close();



    }

