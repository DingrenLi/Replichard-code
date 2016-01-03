/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hdfs.server.namenode;

import Jama.Matrix;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.SrcFac;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.fs.permission.PermissionStatus;
import org.apache.hadoop.hdfs.HDFSPolicyProvider;
import org.apache.hadoop.hdfs.protocol.*;
import org.apache.hadoop.hdfs.server.common.HdfsConstants.StartupOption;
import org.apache.hadoop.hdfs.server.common.IncorrectVersionException;
import org.apache.hadoop.hdfs.server.common.UpgradeStatusReport;
import org.apache.hadoop.hdfs.server.namenode.FSNamesystem.CompleteFileStatus;
import org.apache.hadoop.hdfs.server.namenode.metrics.NameNodeMetrics;
import org.apache.hadoop.hdfs.server.protocol.*;
import org.apache.hadoop.http.HttpServer;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.ipc.Server;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.net.NetworkTopology;
import org.apache.hadoop.security.SecurityUtil;
import org.apache.hadoop.security.UnixUserGroupInformation;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.authorize.*;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.zookeeper.DistrubutedLock;
import org.apache.zookeeper.KeeperException;
import org.zeromq.ZContext;
import org.zeromq.ZFrame;
import org.zeromq.ZMQ;
import org.zeromq.ZMsg;

import java.io.*;
import java.net.InetSocketAddress;
import java.net.URI;
import java.util.*;
import java.util.Date;

import static java.nio.charset.StandardCharsets.UTF_8;

/**********************************************************
 * NameNode serves as both directory namespace manager and
 * "inode table" for the Hadoop DFS.  There is a single NameNode
 * running in any DFS deployment.  (Well, except when there
 * is a second backup/failover NameNode.)
 *
 * The NameNode controls two critical tables:
 *   1)  filename->blocksequence (namespace)
 *   2)  block->machinelist ("inodes")
 *
 * The first table is stored on disk and is very precious.
 * The second table is rebuilt every time the NameNode comes
 * up.
 *
 * 'NameNode' refers to both this class as well as the 'NameNode server'.
 * The 'FSNamesystem' class actually performs most of the filesystem
 * management.  The majority of the 'NameNode' class itself is concerned
 * with exposing the IPC interface and the http server to the outside world,
 * plus some configuration management.
 *
 * NameNode implements the ClientProtocol interface, which allows
 * clients to ask for DFS services.  ClientProtocol is not
 * designed for direct use by authors of DFS client code.  End-users
 * should instead use the org.apache.nutch.hadoop.fs.FileSystem class.
 *
 * NameNode also implements the DatanodeProtocol interface, used by
 * DataNode programs that actually store DFS data blocks.  These
 * methods are invoked repeatedly and automatically by all the
 * DataNodes in a DFS deployment.
 *
 * NameNode also implements the NamenodeProtocol interface, used by
 * secondary namenodes or rebalancing processes to get partial namenode's
 * state, for example partial blocksMap etc.
 **********************************************************/
public class NameNode implements ClientProtocol, DatanodeProtocol,
                                 NamenodeProtocol, FSConstants,
                                 RefreshAuthorizationPolicyProtocol {
  static{
    Configuration.addDefaultResource("hdfs-default.xml");
    Configuration.addDefaultResource("hdfs-site.xml");
  }
  
  public long getProtocolVersion(String protocol, 
                                 long clientVersion) throws IOException { 
    if (protocol.equals(ClientProtocol.class.getName())) {
      return ClientProtocol.versionID; 
    } else if (protocol.equals(DatanodeProtocol.class.getName())){
      return DatanodeProtocol.versionID;
    } else if (protocol.equals(NamenodeProtocol.class.getName())){
      return NamenodeProtocol.versionID;
    } else if (protocol.equals(RefreshAuthorizationPolicyProtocol.class.getName())){
      return RefreshAuthorizationPolicyProtocol.versionID;
    } else {
      throw new IOException("Unknown protocol to name node: " + protocol);
    }
  }
    
  public static final int DEFAULT_PORT = 8020;

    ZContext ctx_dr = new ZContext();
    ZContext ctx_fb= new ZContext();
    ZContext ctx=new ZContext();

    public Map locks=new HashMap();



  public static final Log LOG = LogFactory.getLog(NameNode.class.getName());
    public static final Log LOGLR = LogFactory.getLog("LR.Envaluation");
  public static final Log stateChangeLog = LogFactory.getLog("org.apache.hadoop.hdfs.StateChange");
    private static Random rand = new Random(System.nanoTime());


    public FSNamesystem namesystem; // TODO: This should private. Use getNamesystem() instead.
  /** RPC server */
  private Server server;
  /** RPC server address */
  private InetSocketAddress serverAddress = null;
  /** httpServer */
  private HttpServer httpServer;
  /** HTTP server address */
  private InetSocketAddress httpAddress = null;
  private Thread emptier;
  /** only used for testing purposes  */
  private boolean stopRequested = false;
  /** Is service level authorization enabled? */
  private boolean serviceAuthEnabled = false;
  public   LR lr = new LR();
    int operation_count=0;

  public DistrubutedLock joinGroup = new DistrubutedLock();

  Parameter para;
  public String[] gname={""};
    public int count=20;
    public ArrayList<record> recordList=new ArrayList<record>();
    public ArrayList<record> testList=new ArrayList<record>();
    public ArrayList<readResult> lrRead=new ArrayList<readResult>();
    public ArrayList<readResult> analyzeRead=new ArrayList<readResult>();
    public ArrayList<Parameter> mercleList=new ArrayList<Parameter>();
    String mercleError;
    String add=null;
    String zookeeperAddr;
    String defaultNamenodeDatanode;
    public List<String> clientList=new ArrayList<String>();
    Double requireConf=0.0;
    String rC="";
    int maxRepeate=0;
    String mR="";
    ZMQ.PollItem[] items;
    String nameNum="";
    int nameNumb=0;
    String no="";
    String frontSocket="";
    String backSocket="";
    long startTime;
    long stopTime;
    long intervalTime;
    long Time;


  /** Format a new filesystem.  Destroys any filesystem that may already
   * exist at this location.  **/
  public static void format(Configuration conf) throws IOException {
    format(conf, false);
  }
   public String user="";
  static NameNodeMetrics myMetrics;

  public FSNamesystem getNamesystem() {
    return namesystem;
  }

  public static NameNodeMetrics getNameNodeMetrics() {
    return myMetrics;
  }
  
  public static InetSocketAddress getAddress(String address) {
    return NetUtils.createSocketAddr(address, DEFAULT_PORT);
  }

  public static InetSocketAddress getAddress(Configuration conf) {
    return getAddress(FileSystem.getDefaultUri(conf).getAuthority());
  }

  public static URI getUri(InetSocketAddress namenode) {
    int port = namenode.getPort();
    String portString = port == DEFAULT_PORT ? "" : (":"+port);
    return URI.create("hdfs://"+ namenode.getHostName()+portString);
  }

  /**
   * Initialize name-node.
   * 
   * @param conf the configuration
   */
  private void initialize(Configuration conf) throws IOException {
    InetSocketAddress socAddr = NameNode.getAddress(conf);
    int handlerCount = conf.getInt("dfs.namenode.handler.count", 10);
      this.add=conf.get("fs.default.name");
      LOG.info("add is:"+add);
    this.zookeeperAddr=conf.get("zookeeper.addr");
      this.defaultNamenodeDatanode=conf.get("default.namenode.datanode");
      if (conf.get("required.conf")!=null) rC=conf.get("required.conf");
      if (!rC.isEmpty()){
      this.requireConf=Double.valueOf(rC);}
      if (conf.get("maxRepeat.num")!=null) mR=conf.get("maxRepeat.num");
    if (!mR.isEmpty()){
      this.maxRepeate=Integer.valueOf(mR);}
      LOG.info("rC is:"+requireConf+" rM time is: "+maxRepeate);
      LOG.info("required conf is:"+requireConf+" required maxRepeat time is: "+maxRepeate);
      if (conf.get("namenode.num")!=null) nameNum=conf.get("namenode.num");
      if (!nameNum.isEmpty()){
          this.nameNumb=Integer.valueOf(nameNum);}
    // set service-level authorization security policy
      frontSocket=conf.get("frontend.socket");
       backSocket=conf.get("backend.socket");
      if (conf.get("id")!=null) no=conf.get("id");
      intervalTime=conf.getLong("interval.time",120);
      LOG.info("intervalTime is"+intervalTime);
    if (serviceAuthEnabled = 
          conf.getBoolean(
            ServiceAuthorizationManager.SERVICE_AUTHORIZATION_CONFIG, false)) {
      PolicyProvider policyProvider = 
        (PolicyProvider)(ReflectionUtils.newInstance(
            conf.getClass(PolicyProvider.POLICY_PROVIDER_CONFIG, 
                HDFSPolicyProvider.class, PolicyProvider.class), 
            conf));
      SecurityUtil.setPolicy(new ConfiguredPolicy(conf, policyProvider));
    }

    // create rpc server 
    this.server = RPC.getServer(this, socAddr.getHostName(), socAddr.getPort(),
                                handlerCount, false, conf);


    // The rpc-server port can be ephemeral... ensure we have the correct info
    this.serverAddress = this.server.getListenerAddress(); 
    FileSystem.setDefaultUri(conf, getUri(serverAddress));
    LOG.info("Namenode up at: " + this.serverAddress);
      if (no.equals("1")){
      new Thread() {
          public void run() {
              while (true) {
                      ZMQ.Socket frontend = ctx.createSocket(ZMQ.ROUTER);
                      frontend.bind(frontSocket);
                      ZMQ.Socket backend = ctx.createSocket(ZMQ.DEALER);
                      backend.bind(backSocket);
                      ZMQ.proxy(frontend, backend, null);
              }
      }}.start();}
      server.start_psub();
      server.startClipub();
      myMetrics = new NameNodeMetrics(conf, this);
      this.namesystem = new FSNamesystem(this, conf);
      startHttpServer(conf);
      this.server.start();  //start RPC server
      startTrashEmptier(conf);
      server.start_rdsock(ctx_dr);
      items = new ZMQ.PollItem[] { new ZMQ.PollItem(server.client, ZMQ.Poller.POLLIN) };
      zookeeper(conf);
      try {
          Thread.sleep(5000);
      }catch (InterruptedException E){}
      startTime=new Date().getTime();
      LOG.info("start time is"+startTime);
  new Thread() {
     public void run() {
       while (true) {
           byte[] src = server.subsocket.recv();
          sub_Manager(getPara(src));
       }
     }
    }.start();
      new Thread() {
              public void run() {
                  while (true) {
                      if (recordList != null && recordList.size() > count) {
                          count = count + 20;
                          Format();
                      }
                  }}}.start();
      new Thread() {
          public void run() {
              while (true) {
                      if (((new Date().getTime()-startTime)/1000)>intervalTime&&locks!=null&&locks.containsValue(add.replaceAll("hdfs://", ""))){
                          Set<String> kset=locks.keySet();
                          mercleFac(kset);
                          startTime=startTime+intervalTime*1000;
                      }}}}.start();
      new Thread() {
          public void run() {
              while (true) {
                  ZMsg  msg = ZMsg.recvMsg(server.worker);
              ZFrame address = msg.pop();
                  ZFrame content = msg.pop();
                  assert (content != null);
                  srcMessage message=getMessages(content.getData());
                  msg.destroy();
                  address.send(server.worker, ZFrame.REUSE + ZFrame.MORE);
                  if ((message.parameters==null&&compare(buildOwn(message.src), buildOther(message.mTree)))||message.parameters!=null);
                      content.reset("ok");
                  }else {
                      srcMessage sM=new srcMessage(null,null,null,mercleError);
                      content.reset(setMessage(sM));
                  }
                  content.send(server.worker, ZFrame.REUSE);
                  if (message.parameters!=null){
                      for (Parameter p:message.parameters){
                          sub_Manager(p);
                      }}
                  address.destroy();
                  content.destroy();
                  }}}.start();
        }
    public MerkleTree buildOwn(String[] src){
        MerkleTree S;
        List<String> leaveOwn=new ArrayList<String>();
        for (String s:src){
            leaveOwn.addAll(ls(s));
        }
        if (leaveOwn.size()==1)leaveOwn.add("hello");
        S=new MerkleTree(leaveOwn);
        return S;
    }
    public MerkleTree buildOther(byte[] bye){
        MerkleTree dtree = MerkleDeserializer.deserialize(bye);
        return dtree;
    }


    public List ls(String src){
        List<String> srcsS=new ArrayList<String>();
        FileStatus[] srcs=null;
        try {
             srcs =namesystem.getListing(src);
         }catch (IOException E){
             E.printStackTrace();
         }
        for (FileStatus source:srcs){
                      if (source.isDir())
                        srcsS.addAll(ls(source.getPath().toString()));
                      else srcsS.add(source.getPath().toString());
                      }
        return srcsS;
    }
    public void pubSending(byte[] data){
        for (int i=0;i<nameNumb;i++){
            server.client.send(data,0);}
    }
    public void pubReceiving(){
        ZMQ.poll(items, 100);
        for (int j=0;j<nameNumb;j++) {
            if (items[0].isReadable()) {
                ZMsg msg = ZMsg.recvMsg(server.client);
                byte[] result=msg.pop().getData();
                if (new String(result).equals("ok")) continue;
                  srcMessage srcme=getMessages(result);
                    List<Parameter> replyErro=new ArrayList<Parameter>();
                     for (Parameter m:mercleList){
                         if (m.src.equals(srcme.erro))
                             replyErro.add(m);
                     }
                     srcMessage replyErr=new srcMessage(null,null,new Parameter[replyErro.size()],null);
                    pubSending(setMessage(replyErr));
                    pubReceiving();
                    msg.destroy();
            }}}

     public void test (List<String> leaves,List<String> srcSet){
         if (leaves!=null) {
             if (leaves.size()==1)leaves.add("hello");
             srcMessage sM=new srcMessage(srcSet.toArray(new String[srcSet.size()]),new MerkleTree(leaves).serialize(),null,null);
             pubSending(setMessage(sM));
             pubReceiving();
         }}
    public void mercleFac(Set<String> kset){
        List<String> srcSet=new ArrayList<String>();
        List<String> leaves=new ArrayList<String>();
        for (String ks:kset){
            if (locks.get(ks).equals(add.replaceAll("hdfs://", ""))){
                leaves.addAll(ls(ks));
                srcSet.add(ks);
            }
        }
        test(leaves,srcSet);

    }

  public void zookeeper(Configuration conf) throws IOException {
       try {
         try {
           joinGroup.connect(zookeeperAddr);
         } catch (IOException e) {
           e.printStackTrace();
         }
       } catch (InterruptedException e) {
         e.printStackTrace();
       }
       String groupName = "namenode";
       String path = "/" + groupName;
      try {
        joinGroup.join(path,conf);
       } catch (KeeperException e) {
         e.printStackTrace();
       } catch (InterruptedException e) {
         e.printStackTrace();
       }
      
  }


    boolean compare (MerkleTree S,MerkleTree T)
    {
        if (S==T);
        return true;
      if (S == T)
            return true;
        else{
            boolean result=compareNode(S.getRoot(),T.getRoot());
            assert(result!=false);
            return false;
        }
    }
    public boolean compareNode(MerkleTree.Node S,MerkleTree.Node T){
        if (S!=T&&S.type==MerkleTree.LEAF_SIG_TYPE&&T.type==MerkleTree.LEAF_SIG_TYPE){
             mercleError=new String(T.sig, UTF_8);
            return true;
        }
        else if (S!=T){
            compareNode(S.left,T.left);
            compareNode(S.right,T.right);
        }
          return false;
    }



    private void startTrashEmptier(Configuration conf) throws IOException {
    this.emptier = new Thread(new Trash(conf).getEmptier(), "Trash Emptier");
    this.emptier.setDaemon(true);
    this.emptier.start();
  }

  private void startHttpServer(Configuration conf) throws IOException {
    String infoAddr = 
      NetUtils.getServerAddress(conf, "dfs.info.bindAddress", 
                                "dfs.info.port", "dfs.http.address");
    InetSocketAddress infoSocAddr = NetUtils.createSocketAddr(infoAddr);
    String infoHost = infoSocAddr.getHostName();
    int infoPort = infoSocAddr.getPort();
    this.httpServer = new HttpServer("hdfs", infoHost, infoPort, 
        infoPort == 0, conf);
    if (conf.getBoolean("dfs.https.enable", false)) {
      boolean needClientAuth = conf.getBoolean("dfs.https.need.client.auth", false);
      InetSocketAddress secInfoSocAddr = NetUtils.createSocketAddr(conf.get(
          "dfs.https.address", infoHost + ":" + 0));
      Configuration sslConf = new Configuration(false);
      sslConf.addResource(conf.get("dfs.https.server.keystore.resource",
          "ssl-server.xml"));
      this.httpServer.addSslListener(secInfoSocAddr, sslConf, needClientAuth);
      // assume same ssl port for all datanodes
      InetSocketAddress datanodeSslPort = NetUtils.createSocketAddr(conf.get(
          "dfs.datanode.https.address", infoHost + ":" + 50475));
      this.httpServer.setAttribute("datanode.https.port", datanodeSslPort
          .getPort());
    }
    this.httpServer.setAttribute("name.node", this);
    this.httpServer.setAttribute("name.node.address", getNameNodeAddress());
    this.httpServer.setAttribute("name.system.image", getFSImage());
    this.httpServer.setAttribute("name.conf", conf);
    this.httpServer.addInternalServlet("fsck", "/fsck", FsckServlet.class);
    this.httpServer.addInternalServlet("getimage", "/getimage", GetImageServlet.class);
    this.httpServer.addInternalServlet("listPaths", "/listPaths/*", ListPathsServlet.class);
    this.httpServer.addInternalServlet("data", "/data/*", FileDataServlet.class);
    this.httpServer.addInternalServlet("checksum", "/fileChecksum/*",
        FileChecksumServlets.RedirectServlet.class);
    this.httpServer.start();

    // The web-server port can be ephemeral... ensure we have the correct info
    infoPort = this.httpServer.getPort();
    this.httpAddress = new InetSocketAddress(infoHost, infoPort);
    conf.set("dfs.http.address", infoHost + ":" + infoPort);
    LOG.info("Web-server up at: " + infoHost + ":" + infoPort);
  }

  public void pub_Manager(Parameter para) throws IOException {
      server.pubsocket.send(setPara(para));

  }

    public void Format(){
        ArrayList<record> rL= new ArrayList<record>();
        rL.addAll(recordList);
        testList.addAll(recordList);
        //recordList.clear();

        Collections.sort(rL,new sort());
        ArrayList<testSet> testSets=new ArrayList<testSet>();
        for (int j=0;j<rL.size();j++){
            if (rL.get(j).read==0) {
                int read_fre=-1;
                int write_fre=-1;
                long time_dis=-1;
                int result=-1;
                boolean t_dis=true;
                for (int i=j-1;i>=0;i--){
                    if (rL.get(i).src.equals(rL.get(j).src)) {
                        if (rL.get(i).read == 1) write_fre++;
                        if (rL.get(i).read == 0) read_fre++;
                        if (rL.get(i).read==2&&t_dis){
                            time_dis=(rL.get(j).time-rL.get(i).time);
                            t_dis=false;}}}
                if (j>2&&(j+2<rL.size())&&(rL.get(j - 1).src.equals( rL.get(j).src) ||
                        rL.get(j - 2).src.equals(rL.get(j).src)) &&
                        (rL.get(j - 1).read == 1 || rL.get(j - 2).read == 1)) {
                            if ((rL.get(j + 1).src.equals(rL.get(j).src) ||
                                    rL.get(j + 2).src.equals(rL.get(j).src))&&
                                    (rL.get(j + 1).read == 2 || rL.get(j + 2).read == 2)) {
                                result = 0;
                            }}
                else result=1;
                testSets.add(new testSet(read_fre,write_fre,time_dis,result));
                analyzeRead.add(new readResult(rL.get(j).src,result));
                if (lrRead.get(analyzeRead.size()-1).src.equals(rL.get(j).src)){
                    if (lrRead.get(analyzeRead.size()-1).result!=result) {
                        server.cliPub.send(lrRead.get(analyzeRead.size() - 1).src.getBytes());
                        LOG.info("Sending wrong result");
                    }}}}
        Matrix dataMat= lr.getDataMat(testSets);
        Matrix labelMat= lr.getLabelMat(testSets);
        lr.analyze(dataMat,labelMat);
      }


    class sort implements Comparator{
        public int compare(Object o1,Object o2){
            record r1=(record)o1;
            record r2=(record)o2;
            if (r1.time>r2.time) return 1;
            return -1;
        }
    }


    public byte[] setMessage(srcMessage message){
        ByteArrayOutputStream baos = new ByteArrayOutputStream(1024);
        DataOutputStream out = new DataOutputStream(baos);
        try{ message.write(out);}
        catch (IOException E){
            E.printStackTrace();
        }
        byte[] byt = baos.toByteArray();
        try {
            out.close();
            baos.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return byt;
    }


    public byte[] setPara(Parameter para){
        ByteArrayOutputStream baos = new ByteArrayOutputStream(1024);
        DataOutputStream out = new DataOutputStream(baos);
        try{
            para.write(out);}
        catch (IOException E){
            E.printStackTrace();
        }
        byte[] byt = baos.toByteArray();
        try {
            out.close();
            baos.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return byt;
    }


    public  Parameter getPara(byte[] by){
        ByteArrayInputStream paos = new ByteArrayInputStream(by);
        DataInputStream in=new DataInputStream(paos);
        Parameter  par=new Parameter();
        //  LOG.info("before read");
        try {
            par.readFields(in);
            in.close();
            paos.close();
        }catch (IOException e){
            LOG.info(e.getCause());
            e.printStackTrace();
        }
        return par;
    }
    public srcMessage getMessages(byte[] by){
        ByteArrayInputStream paos = new ByteArrayInputStream(by);
        DataInputStream in=new DataInputStream(paos);
        srcMessage srcmessage=new srcMessage();
        try {
            srcmessage.readFields(in);
            in.close();
            paos.close();
        }catch (IOException e){
            LOG.info(e.getCause());
            e.printStackTrace();
        }
        return srcmessage;
    }

  public  void sub_Manager(Parameter parameter){

      //LOG.info("after read");
      int methodname=parameter.get_methodname();
      long time=parameter.get_time();
      String maprejobname=parameter.get_jobname();
      String src=parameter.get_src();
      try {
      switch (methodname){
         case 1:{FsPermission masked=parameter.get_mask_permmition();
               String clientName=parameter.get_clientname_dist();
               boolean overwrite=parameter.get_overwrite();
               short replication=parameter.get_replication();
               long blockSize=parameter.get_blocksize();
               user=parameter.get_user();
               gname=parameter.get_Gname();
              LOG.info("evafile.  " + "type is:" + 1 + " src is:" + src + " Date is:" + new Date().getTime());
               recordList.add(new record(1,src,time));
               create(src, masked, clientName, overwrite, replication, blockSize);
               break;}
        case 2:{String cname=parameter.get_clientname_dist();
          LOG.info("evafile.  " + "type is:" + 1 + " src is:" + src + " Date is:" + new Date().getTime());
            recordList.add(new record(1,src,time));
                append(src,cname);
                break;}
        case 3:{short rep=parameter.get_replication();
          LOG.info("evafile.  " + "type is:" + 1 + " src is:" + src + " Date is:" + new Date().getTime());
                recordList.add(new record(1,src,time));
               setReplication(src,rep);
                break;}
        case 4:{FsPermission permission  =parameter.get_mask_permmition();
           LOG.info("evafile.  " + "type is:" + 1 + " src is:" + src + " Date is:" + new Date().getTime());
                recordList.add(new record(1,src,time));
                setPermission(src, permission);
                break;}
        case 5:{String username=parameter.get_usrname();
                String groupname=parameter.get_groupname();
           LOG.info("evafile.  " + "type is:" + 1 + " src is:" + src + " Date is:" + new Date().getTime());
                recordList.add(new record(1,src,time));
                setOwner(src,username,groupname);
                break;}
        case 6:{Block b=parameter.get_Block();
                String holder=parameter.get_holder();
           LOG.info("evafile.  " + "type is:" + 1 + " src is:" + src + " Date is:" + new Date().getTime());
                recordList.add(new record(1,src,time));
                abandonBlock(b,src,holder);
                break;}
        case 7:{String cliname=parameter.get_clientname_dist();
                namesystem.set_pub_block(parameter.get_Block());
           LOG.info("evafile.  " + "type is:" + 1 + " src is:" + src + " Date is:" + new Date().getTime());
                recordList.add(new record(1,src,time));
                addBlock(src,cliname);
                break;}
        case 8:{String clname=parameter.get_clientname_dist();
           LOG.info("evafile.  " + "type is:" + 1 + " src is:" + src + " Date is:" + new Date().getTime());
                recordList.add(new record(1,src,time));
                complete(src,clname);
                break;}
        case 9:{String dist=parameter.get_clientname_dist();
           LOG.info("evafile.  " + "type is:" + 1 + " src is:" + src + " Date is:" + new Date().getTime());
                recordList.add(new record(1,src,time));
                rename(src,dist);
                break;}
        case 10:{
           LOG.info("evafile.  " + "type is:" + 1 + " src is:" + src + " Date is:" + new Date().getTime());
                 recordList.add(new record(1,src,time));
                 delete(src);
                 break;}
        case 11:{boolean recursive=parameter.get_recursive();
                 user=parameter.get_user();
                 gname=parameter.get_Gname();
           LOG.info("evafile.  " + "type is:" + 1 + " src is:" + src + " Date is:" + new Date().getTime());
                 recordList.add(new record(1,src,time));
                 delete(src,recursive);
                 break;}
        case 12:{FsPermission mask=parameter.get_mask_permmition();
                 user=parameter.get_user();
                 gname=parameter.get_Gname();
           LOG.info("evafile.  " + "type is:" + 1 + " src is:" + src + " Date is:" + new Date().getTime());
                 recordList.add(new record(1,src,time));
                 mkdirs(src,mask);
                 break;}
        case 13:{
           LOG.info("evafile.  " + "type is:" + 1 + " src is:" + src + " Date is:" + new Date().getTime());
                 recordList.add(new record(1,src,time));
                if (!clientList.contains(src))clientList.add(src);
                 renewLease(src);
                 break;}
        case 14:{
                 register(parameter.get_nodeReg());
                 break;}
        case 15:{
                 sendHeartbeat(parameter.get_nodeReg(),parameter.get_capacity(),parameter.get_dfsUsed(),parameter.get_remaining(),parameter.get_xmitsInProgress(),parameter.get_xceiverCount());
                 break;}
        case 16:{
                 blockReport(parameter.get_nodeReg(),parameter.get_blocks());
                 break;}
        case 17:{
                 blockReceived(parameter.get_nodeReg(),parameter.get_block(),parameter.get_delHints());
                 break;}
        case 18:{
                 errorReport(parameter.get_nodeReg(),parameter.get_errorCode(),parameter.get_msg());
                 break;}
        case 19:{
                 processUpgradeCommand(parameter.get_comm());
                 break;}
        case 20:{
                 reportBadBlocks(parameter.get_bloc());
                 break;}
    }}catch (IOException e){
      e.printStackTrace();}}

  /**
   * Start NameNode.
   * <p>
   * The name-node can be started with one of the following startup options:
   * <ul> 
   * <li>{@link StartupOption#REGULAR REGULAR} - normal name node startup</li>
   * <li>{@link StartupOption#FORMAT FORMAT} - format name node</li>
   * <li>{@link StartupOption#UPGRADE UPGRADE} - start the cluster  
   * upgrade and create a snapshot of the current file system state</li> 
   * <li>{@link StartupOption#ROLLBACK ROLLBACK} - roll the  
   *            cluster back to the previous state</li>
   * </ul>
   * The option is passed via configuration field: 
   * <tt>dfs.namenode.startup</tt>
   * 
   * The conf will be modified to reflect the actual ports on which 
   * the NameNode is up and running if the user passes the port as
   * <code>zero</code> in the conf.
   * 
   * @param conf  confirguration
   * @throws IOException
   */
  public NameNode(Configuration conf) throws IOException {
    try {initialize(conf);
    } catch (IOException e) {
        this.stop();
        throw e;
    }
  }

  /**
   * Wait for service to finish.
   * (Normally, it runs forever.)
   */
  public void join() {
    try {
      this.server.join();
    } catch (InterruptedException ie) {
      LOG.info("close");
    }
  }

  /**
   * Stop all NameNode threads and wait for all to finish.
   */
  public void stop() {
  try {
      joinGroup.close();
    } catch (InterruptedException e1) {
      e1.printStackTrace();
    }
      if (stopRequested)
      return;
    stopRequested = true;
    try {
      if (httpServer != null) httpServer.stop();
    } catch (Exception e) {
      LOG.error(StringUtils.stringifyException(e));
    }
    if(namesystem != null) namesystem.close();
    if(emptier != null) emptier.interrupt();
    if(server != null) server.stop();
      if (ctx_dr!=null) ctx_dr.destroy();
      if (ctx_fb!=null)  ctx_fb.destroy();
    if (myMetrics != null) {
      myMetrics.shutdown();
    }
    if (namesystem != null) {
      namesystem.shutdown();
    }
  }

    public boolean check_locks(String src) {
        if (locks.containsKey(src)) {
            if (locks.get(src).equals(add.replaceAll("hdfs://", "")))
                return true;
            else return false;
        } else {
            DistrubutedLock joinGroup = new DistrubutedLock();
            try {
                joinGroup.connect(zookeeperAddr);
                locks.putAll(joinGroup.get_locks(src));
                namesystem.setLocks(locks);
                joinGroup.close();
                if (locks.containsKey(src)) {
                    if (locks.get(src).equals(add.replaceAll("hdfs://", ""))) {
                        return true;
                    }
                } else return false;
            } catch (InterruptedException E) {
                E.printStackTrace();
            } catch (KeeperException E) {
                E.printStackTrace();
            } catch (IOException E) {
                E.printStackTrace();
            }
        }
        return false;
    }
    public boolean Check(String mapredname,String src,long time){

        if (!check_locks(src)&&(!src.equals("null"))){//no having lock
            int read_fre=-1;
            int write_fre=-1;
            long time_dis =-1;

            boolean t_dis=true;
            testList.addAll(recordList);
            int j=testList.size();
           if (j>20){
            for (int i=j-1;i>j-20;i--){
                if (testList.get(i).src.equals(src)) {
                    if (testList.get(i).read == 1) write_fre++;
                    if (testList.get(i).read == 0) read_fre++;
                    if (testList.get(i).read==2&&t_dis){
                        time_dis=(time-testList.get(i).time);
                        t_dis=false;}}}
            double confi=get_confi(read_fre,write_fre,time_dis);
                if (confi>requireConf){
                LOG.info("ficonfi.  "+"time is:"+new Date().getTime()+" jobname is:"+mapredname
                        +" src is:"+src+" confi is:"+confi);
                return true;
            } else return false;
        } else {
             LOG.info("ficonfi.  " + src + " test list less than 20");
               return true;
            }
        } else {
            if (src=="null"){
            LOG.info("ficonfi.  " + "src is null  "+"time is:" + new Date().getTime() + " jobname is:" + mapredname
                    + " src is:" + src + " confi is" + 1);
        }else LOG.info("ficonfi.  " +"having the lock "+ "time is:" + new Date().getTime() + " jobname is:" + mapredname
                + " src is:" + src + " confi is" + 1);
            return true;}
    }
    public double get_confi(int read_fre,int write_fre,long time_dis)
    {
        if (recordList.size()>10){
        double[][] data = new double[1][3];
        data[0][0]=read_fre;
        data[0][1]=write_fre;
        data[0][2]=time_dis;
        Matrix dataM=new Matrix(data);
        double conf=lr.test(dataM);
        return conf;}
        else{  LOG.info("confile.  " + "record list is less than 10"); return 0.7;}
    }

  /////////////////////////////////////////////////////
  // NamenodeProtocol
  /////////////////////////////////////////////////////
  /**
   * return a list of blocks & their locations on <code>datanode</code> whose
   * total size is <code>size</code>
   * 
   * @param datanode on which blocks are located
   * @param size total size of blocks
   */
  public BlocksWithLocations getBlocks(DatanodeInfo datanode, long size)
  throws IOException {
    if(size <= 0) {
      throw new IllegalArgumentException(
        "Unexpected not positive size: "+size);
    }
      return namesystem.getBlocks(datanode, size);
  }
  
  /////////////////////////////////////////////////////
    // ClientProtocol
    /////////////////////////////////////////////////////
  /** {@inheritDoc} */
  public LocatedBlocks   getBlockLocations(String src, 
                                          long offset,
                                          long length) throws IOException {


      long time=-1;
      LocatedBlocks b=null;
      myMetrics.numGetBlockLocations.inc();
      b=namesystem.getBlockLocations(getClientMachine(), src, offset, length);
      time=new Date().getTime();
      if (coun<=maxRepeate) lrRead.add(new readResult(src,1));
      LOG.info("confile.  "+"time is:"+new Date().getTime()+" jobname is:"+server.maprJobName
                +" src is:"+src);
     LOG.info("evafile.  " + "type is:" + 0 + " src is" + src + " Date is" + new Date().getTime());
      recordList.add(new record(0, src, time));
      stopTime=new Date().getTime();
      LOG.info("stopTime is:"+stopTime);
       return b;
  }
  
  private static String getClientMachine() {
    String clientMachine = Server.getRemoteAddress();
    if (clientMachine == null) {
      clientMachine = "";
    }
    return clientMachine;
  }
  
  /** {@inheritDoc} */
  public void create(String src, 
                     FsPermission masked,
                             String clientName, 
                             boolean overwrite,
                             short replication,
                             long blockSize
                             ) throws IOException {

    String clientMachine = getClientMachine();

    if (stateChangeLog.isDebugEnabled()) {
      stateChangeLog.debug("*DIR* NameNode.create: file "
                         +src+" for "+clientName+" at "+clientMachine);
    }

    if (!checkPathLength(src)) {
      throw new IOException("create: Pathname too long.  Limit " 
                            + MAX_PATH_LENGTH + " characters, " + MAX_PATH_DEPTH + " levels.");}


     if (!check_locks(SrcFac.srcfac(src))){
        UnixUserGroupInformation ugi=new UnixUserGroupInformation(user,gname);
      UserGroupInformation.setCurrentUser(ugi);
     }

      PermissionStatus permission = new PermissionStatus(UserGroupInformation.getCurrentUGI().getUserName(),
              null, masked);
      if (!check_locks(SrcFac.srcfac(src))){
         LOG.info("evafile.  " + "type is:" + 2 + " src is" + src + " Date is" + new Date().getTime());
      recordList.add(new record(2,src,new Date().getTime()));}
      namesystem.startFile(src,permission,
              clientName, clientMachine, overwrite, replication, blockSize);



      if (check_locks(SrcFac.srcfac(src))) {
          para = new Parameter(src, masked, clientName, overwrite, replication, blockSize,
                UserGroupInformation.getCurrentUGI().getUserName(),
                UserGroupInformation.getCurrentUGI().getGroupNames());
                long l = new Date().getTime();
               para.Parameter_M(1, l, server.maprJobName);
                pub_Manager(para);
                mercleList.add(para);
                }

    myMetrics.numFilesCreated.inc();
    myMetrics.numCreateFileOps.inc();
      stopTime=new Date().getTime();
      LOG.info("stopTime is:" + stopTime);
  }

  /** {@inheritDoc} */
  public LocatedBlock append(String src, String clientName) throws IOException {
    String clientMachine = getClientMachine();
    if (stateChangeLog.isDebugEnabled()) {
      stateChangeLog.debug("*DIR* NameNode.append: file "
          +src+" for "+clientName+" at "+clientMachine);
    }
    LocatedBlock info = namesystem.appendFile(src, clientName, clientMachine);
      if (!check_locks(SrcFac.srcfac(src))){
         LOG.info("evafile.  " + "type is:" + 2 + " src is" + src + " Date is" + new Date().getTime());
      recordList.add(new record(2,src,new Date().getTime()));
      }
    if (check_locks(SrcFac.srcfac(src))){
    para=new Parameter(src,clientName);
    para.Parameter_M(2,new Date().getTime(),server.maprJobName);
    pub_Manager(para);
    mercleList.add(para);}
    myMetrics.numFilesAppended.inc();
      stopTime=new Date().getTime();
      LOG.info("stopTime is:" + stopTime);
      return info;

  }

  /** {@inheritDoc} */
  public boolean setReplication(String src, 
                                short replication
                                ) throws IOException {

    Boolean b=namesystem.setReplication(src, replication);
      if (!check_locks(SrcFac.srcfac(src))){
         LOG.info("evafile.  " + "type is:" + 2 + " src is" + src + " Date is" + new Date().getTime());
      recordList.add(new record(2,src,new Date().getTime()));}
    if (check_locks(SrcFac.srcfac(src))){
    para=new Parameter(src,replication);
    para.Parameter_M(3,new Date().getTime(),server.maprJobName);
    pub_Manager(para);
    mercleList.add(para);}
      stopTime=new Date().getTime();
      LOG.info("stopTime is:"+stopTime);
    return b;
  }
    
  /** {@inheritDoc} */
  public void setPermission(String src, FsPermission permissions
      ) throws IOException {
    namesystem.setPermission(src, permissions);
      if (!check_locks(SrcFac.srcfac(src))){
         LOG.info("evafile.  " + "type is:" + 2 + " src is" + src + " Date is" + new Date().getTime());
      recordList.add(new record(2,src,new Date().getTime()));}
    if (check_locks(SrcFac.srcfac(src))){
    para=new Parameter(src,permissions);
    para.Parameter_M(4,new Date().getTime(),server.maprJobName);
    pub_Manager(para);
    mercleList.add(para);}
      stopTime=new Date().getTime();
      LOG.info("stopTime is:"+stopTime);
  }

  /** {@inheritDoc} */
  public void setOwner(String src, String username, String groupname
      ) throws IOException {
    namesystem.setOwner(src, username, groupname);
      if (!check_locks(SrcFac.srcfac(src))){
         LOG.info("evafile.  " + "type is:" + 2 + " src is" + src + " Date is" + new Date().getTime());
      recordList.add(new record(2,src,new Date().getTime()));}
    if (check_locks(SrcFac.srcfac(src))){
    para =new Parameter(src,username);
    para.Parameter_M(5,new Date().getTime(),server.maprJobName);
    pub_Manager(para);
    mercleList.add(para);}
      time=new Date().getTime();
      LOG.info("stopTime is:"+stopTime);
  }

  /**
   */
  public LocatedBlock addBlock(String src, 
                               String clientName) throws IOException {
    stateChangeLog.debug("*BLOCK* NameNode.addBlock: file "
                         +src+" for "+clientName);
      LocatedBlock locatedBlock = namesystem.getAdditionalBlock(src, clientName);
      if (!check_locks(SrcFac.srcfac(src))){
         LOG.info("evafile.  " + "type is:" + 2 + " src is" + src + " Date is" + new Date().getTime());
      recordList.add(new record(2,src,new Date().getTime()));
    }
    if (locatedBlock != null)
      myMetrics.numAddBlockOps.inc();

    if (check_locks(SrcFac.srcfac(src))){
        para =new Parameter(src,clientName,locatedBlock.getBlock());
    para.Parameter_M(7,new Date().getTime(),server.maprJobName);
    pub_Manager(para);
    mercleList.add(para);}
      stopTime=new Date().getTime();
      LOG.info("stopTime is:"+stopTime);
      return locatedBlock;
  }

  /**
   * The client needs to give up on the block.
   */
  public void abandonBlock(Block b, String src, String holder
      ) throws IOException {
    stateChangeLog.debug("*BLOCK* NameNode.abandonBlock: "
                         +b+" of file "+src);
      if (!check_locks(SrcFac.srcfac(src))){
         LOG.info("evafile.  " + "type is:" + 2 + " src is" + src + " Date is" + new Date().getTime());
      recordList.add(new record(2,src,new Date().getTime()));
     }
    if (!namesystem.abandonBlock(b, src, holder)) {
      throw new IOException("Cannot abandon block during write to " + src);
    }

    if (check_locks(SrcFac.srcfac(src))){
    para=new Parameter(b,src,holder);
    para.Parameter_M(6,new Date().getTime(),server.maprJobName);
    pub_Manager(para);
    mercleList.add(para);}
      stopTime=new Date().getTime();
      LOG.info("stopTime is:"+time);
  }

  /** {@inheritDoc} */
  public boolean complete(String src, String clientName) throws IOException {
    stateChangeLog.debug("*DIR* NameNode.complete: " + src + " for " + clientName);
    CompleteFileStatus returnCode = namesystem.completeFile(src, clientName);
      if (!check_locks(SrcFac.srcfac(src))){
         LOG.info("evafile.  " + "type is:" + 2 + " src is" + src + " Date is" + new Date().getTime());
      recordList.add(new record(2,src,new Date().getTime()));
      }
    if (check_locks(SrcFac.srcfac(src))&&(returnCode == CompleteFileStatus.COMPLETE_SUCCESS)){
      para=new Parameter(src,clientName);
    para.Parameter_M(8,new Date().getTime(),server.maprJobName);
    pub_Manager(para);
    mercleList.add(para);}

    if (returnCode == CompleteFileStatus.STILL_WAITING) {
      return false;
    } else if (returnCode == CompleteFileStatus.COMPLETE_SUCCESS) {
      return true;
    } else {
      throw new IOException("Could not complete write to file " + src + " by " + clientName);
    }
      stopTime=new Date().getTime();
      LOG.info("stopTime is:"+time);
  }

  /**
   * The client has detected an error on the specified located blocks 
   * and is reporting them to the server.  For now, the namenode will 
   * mark the block as corrupt.  In the future we might 
   * check the blocks are actually corrupt. 
   */
  public void reportBadBlocks(LocatedBlock[] blocks) throws IOException {
    stateChangeLog.info("*DIR* NameNode.reportBadBlocks");
    for (int i = 0; i < blocks.length; i++) {
      Block blk = blocks[i].getBlock();
      DatanodeInfo[] nodes = blocks[i].getLocations();
      for (int j = 0; j < nodes.length; j++) {
        DatanodeInfo dn = nodes[j];
        namesystem.markBlockAsCorrupt(blk, dn);
      }
    }
    if (add.equals(defaultNamenodeDatanode)){
      para=new Parameter(blocks);
      para.Parameter_M(20,new Date().getTime(),server.maprJobName);
      pub_Manager(para);
        mercleList.add(para);
    }
      stopTime=new Date().getTime();
      LOG.info("stopTime is:"+stopTime);
  }

  /** {@inheritDoc} */
  public long nextGenerationStamp(Block block) throws IOException{
    return namesystem.nextGenerationStampForBlock(block);
  }

  /** {@inheritDoc} */
  public void commitBlockSynchronization(Block block,
      long newgenerationstamp, long newlength,
      boolean closeFile, boolean deleteblock, DatanodeID[] newtargets
      ) throws IOException {
    namesystem.commitBlockSynchronization(block,
        newgenerationstamp, newlength, closeFile, deleteblock, newtargets);
  }
  
  public long getPreferredBlockSize(String filename) throws IOException {
      long size=-1;
      long time=-1;
      int coun=0;
      coun++;
      size=namesystem.getPreferredBlockSize(filename);
      time=new Date().getTime();
      if (coun<=maxRepeate) lrRead.add(new readResult(filename,1));
      LOG.info("confile.  " + "time is:" + new Date().getTime() + " jobname is:" + server.maprJobName
              + " src is:" + filename);
      LOG.info("evafile.  " + "type is:" + 0 + " src is" + filename + " Date is" + new Date().getTime());
      recordList.add(new record(0, filename, time));
    return size;
      stopTime=new Date().getTime();
      LOG.info("stopTime is:" + stopTime);
  }
    
  /**
   */
  public boolean rename(String src, String dst) throws IOException {
    stateChangeLog.debug("*DIR* NameNode.rename: " + src + " to " + dst);
    if (!checkPathLength(dst)) {
      throw new IOException("rename: Pathname too long.  Limit " 
                            + MAX_PATH_LENGTH + " characters, " + MAX_PATH_DEPTH + " levels.");
    }
    boolean ret = namesystem.renameTo(src, dst);
      if (!check_locks(SrcFac.srcfac(src))){
         LOG.info("evafile.  " + "type is:" + 2 + " src is" + src + " Date is" + new Date().getTime());
      recordList.add(new record(2,src,new Date().getTime()));}
    if (ret) {
      myMetrics.numFilesRenamed.inc();
    }
    if (check_locks(SrcFac.srcfac(src))){
    para =new Parameter(src,dst);
    para.Parameter_M(9,new Date().getTime(),server.maprJobName);
    pub_Manager(para);
    mercleList.add(para);}
      stopTime=new Date().getTime();
      LOG.info("stopTime is:"+stopTime);
      return ret;
  }

  /**
   */
  @Deprecated
  public boolean delete(String src) throws IOException {

       boolean b=delete(src, true);
      if (!check_locks(SrcFac.srcfac(src))){
         LOG.info("evafile.  " + "type is:" + 2 + " src is" + src + " Date is" + new Date().getTime());
      recordList.add(new record(2,src,new Date().getTime()));}
    if (check_locks(SrcFac.srcfac(src))){
    para =new Parameter(src);
    para.Parameter_M(10,new Date().getTime(),server.maprJobName);
    pub_Manager(para);
    mercleList.add(para);}
      stopTime=new Date().getTime();
      LOG.info("stopTime is:"+stopTime);
      return b;
  }

  /** {@inheritDoc} */
  public boolean delete(String src, boolean recursive) throws IOException {
    if (stateChangeLog.isDebugEnabled()) {
      stateChangeLog.debug("*DIR* Namenode.delete: src=" + src
          + ", recursive=" + recursive);
    }
    if(!check_locks(SrcFac.srcfac(src))){
    namesystem.setuser(user);
      namesystem.set_gname(gname);}

    boolean ret = namesystem.delete(src, recursive);
      if (!check_locks(SrcFac.srcfac(src))){
         LOG.info("evafile.  " + "type is:" + 2 + " src is" + src + " Date is" + new Date().getTime());
      recordList.add(new record(2,src,new Date().getTime()));}
      if (ret)
      myMetrics.numDeleteFileOps.inc();
    if (check_locks(SrcFac.srcfac(src))){
      user=namesystem.get_user();
      gname=namesystem.get_gname();
        para =new Parameter(src,recursive,user,gname);
    para.Parameter_M(11,new Date().getTime(),server.maprJobName);
    pub_Manager(para);
    mercleList.add(para);}
      stopTime=new Date().getTime();
      LOG.info("stopTime is:"+stopTime);
      return ret;
  }

  /**
   * Check path length does not exceed maximum.  Returns true if
   * length and depth are okay.  Returns false if length is too long 
   * or depth is too great.
   * 
   */
  private boolean checkPathLength(String src) {
    Path srcPath = new Path(src);
    return (src.length() <= MAX_PATH_LENGTH &&
            srcPath.depth() <= MAX_PATH_DEPTH);
  }
    
  /** {@inheritDoc} */
  public boolean mkdirs(String src, FsPermission masked) throws IOException {
    String user1=null;
    String groupname[]=null;


    stateChangeLog.debug("*DIR* NameNode.mkdirs: " + src);
    if (!checkPathLength(src)) {
      throw new IOException("mkdirs: Pathname too long.  Limit " 
                            + MAX_PATH_LENGTH + " characters, " + MAX_PATH_DEPTH + " levels.");
    }
    try {
      user1 = UserGroupInformation.getCurrentUGI().getUserName();
      groupname= UserGroupInformation.getCurrentUGI().getGroupNames();
    }catch (RuntimeException e) {
      UnixUserGroupInformation ugi=new UnixUserGroupInformation(user,gname);
      UserGroupInformation.setCurrentUser(ugi);
      user1=user;
    } finally {
      PermissionStatus permission = new PermissionStatus(user1,
              null, masked);
      boolean i = namesystem.mkdirs(src, permission);
        if (!check_locks(SrcFac.srcfac(src))) {
           LOG.info("evafile.  " + "type is:" + 2 + " src is" + src + " Date is" + new Date().getTime());
            recordList.add(new record(2, src, new Date().getTime()));
        }
      if (check_locks(SrcFac.srcfac(src))){
      para =new Parameter(src,masked,user1,groupname);
      para.Parameter_M(12,new Date().getTime(),server.maprJobName);
      pub_Manager(para);
      mercleList.add(para);}
      return i;
        stopTime=new Date().getTime();
        LOG.info("stopTime is:"+stopTime);
    }}



  /**
   */
  public void renewLease(String clientName) throws IOException {
    namesystem.renewLease(clientName);
      if (clientList.contains(clientName)){
         LOG.info("evafile.  " + "type is:" + 2 + " src is" + clientName + " Date is" + new Date().getTime());
      recordList.add(new record(2,clientName,new Date().getTime()));}
    if (!clientList.contains(clientName)){
      para=new Parameter(clientName);
      para.Parameter_M(13,new Date().getTime(),server.maprJobName);
      pub_Manager(para);
        mercleList.add(para);
        stopTime=new Date().getTime();
        LOG.info("stopTime is:" + stopTime);
    }
  }



  /**
   */
  public FileStatus[] getListing(String src) throws IOException {
      FileStatus[] files=null;
      long time=-1;
      files= namesystem.getListing(src);
      time=new Date().getTime();
      if (files != null) {
          myMetrics.numGetListingOps.inc();
          }
      if (coun<=maxRepeate) lrRead.add(new readResult(src,1));
      LOG.info("confile.  " + "time is:" + new Date().getTime() + " jobname is:" + server.maprJobName
              + " src is:" + src);
     LOG.info("evafile.  " + "type is:" + 0 + " src is" + src + " Date is" + new Date().getTime());
      recordList.add(new record(0, src, time));
      stopTime=new Date().getTime();
      LOG.info("stopTime is:" + stopTime);
      return files;
  }

  /**
   * Get the file info for a specific file.
   * @param src The string representation of the path to the file
   * @throws IOException if permission to access file is denied by the system
   * @return object containing information regarding the file
   *         or null if file not found
   */
  public FileStatus getFileInfo(String src)  throws IOException {
      FileStatus fileStatus=null;
      long time=-1;
      myMetrics.numFileInfoOps.inc();
      fileStatus=namesystem.getFileInfo(src);
      time=new Date().getTime();
      if (coun<=maxRepeate) lrRead.add(new readResult(src,1));
      LOG.info("confile.  " + "time is:" + new Date().getTime() + " jobname is:" + server.maprJobName
              + " src is:" + src);
     LOG.info("evafile.  " + "type is:" + 0 + " src is" + src + " Date is" + new Date().getTime());
      recordList.add(new record(0, src, time));
      stopTime=new Date().getTime();
      LOG.info("stopTime is:" + stopTime);
      return fileStatus;
  }

  /** @inheritDoc */
  public long[] getStats() throws IOException {
      long[] longs=null;
      long time=-1;
      longs=namesystem.getStats();
      time=new Date().getTime();
      if (coun<=maxRepeate) lrRead.add(new readResult(null,1));
      LOG.info("confile.  " + "time is:" + new Date().getTime() + " jobname is:" + server.maprJobName
              + " src is:" + "null");
     LOG.info("evafile.  " + "type is:" + 0 + " src is" + "null" + " Date is" + new Date().getTime());
      stopTime=new Date().getTime();
      LOG.info("stopTime is:"+stopTime);
    return longs;
  }

  /**
   */
  public DatanodeInfo[] getDatanodeReport(DatanodeReportType type)
  throws IOException {
      DatanodeInfo results[]=null;
      long time=-1;
      int coun=0;
      results= namesystem.datanodeReport(type);
      time=new Date().getTime();
      if (coun<=maxRepeate) lrRead.add(new readResult(type.toString(),1));

      LOG.info("confile.  " + "time is:" + new Date().getTime() + " jobname is:" + server.maprJobName
              + " src is:" + "null");
     LOG.info("evafile.  " + "type is:" + 0 + " src is" + "null" + " Date is" + new Date().getTime());


    if (results == null ) {
      throw new IOException("Cannot find datanode report");
    }
      stopTime=new Date().getTime();
      LOG.info("stopTime is:"+stopTime);
    return results;
  }
    
  /**
   * @inheritDoc
   */
  public boolean setSafeMode(SafeModeAction action) throws IOException {
    return namesystem.setSafeMode(action);
  }

  /**
   * Is the cluster currently in safe mode?
   */
  public boolean isInSafeMode() {
    return namesystem.isInSafeMode();
  }

  /**
   * @inheritDoc
   */
  public void saveNamespace() throws IOException {
    namesystem.saveNamespace();
  }

  /**
   * Refresh the list of datanodes that the namenode should allow to  
   * connect.  Re-reads conf by creating new Configuration object and 
   * uses the files list in the configuration to update the list. 
   */
  public void refreshNodes() throws IOException {
    namesystem.refreshNodes(new Configuration());
  }

  /**
   * Returns the size of the current edit log.
   */
  public long getEditLogSize() throws IOException {
    return namesystem.getEditLogSize();
  }

  /**
   * Roll the edit log.
   */
  public CheckpointSignature rollEditLog() throws IOException {
    return namesystem.rollEditLog();
  }

  /**
   * Roll the image 
   */
  public void rollFsImage() throws IOException {
    namesystem.rollFSImage();
  }
    
  public void finalizeUpgrade() throws IOException {
    namesystem.finalizeUpgrade();
  }

  public UpgradeStatusReport distributedUpgradeProgress(UpgradeAction action
                                                        ) throws IOException {
    return namesystem.distributedUpgradeProgress(action);
  }

  /**
   * Dumps namenode state into specified file
   */
  public void metaSave(String filename) throws IOException {
    namesystem.metaSave(filename);
  }

  /** {@inheritDoc} */
  public ContentSummary getContentSummary(String path) throws IOException {
    return namesystem.getContentSummary(path);
  }

  /** {@inheritDoc} */
  public void setQuota(String path, long namespaceQuota, long diskspaceQuota) 
                       throws IOException {
    namesystem.setQuota(path, namespaceQuota, diskspaceQuota);
  }
  
  /** {@inheritDoc} */
  public void fsync(String src, String clientName) throws IOException {
    namesystem.fsync(src, clientName);
  }

  /** @inheritDoc */
  public void setTimes(String src, long mtime, long atime) throws IOException {
    namesystem.setTimes(src, mtime, atime);
  }

  ////////////////////////////////////////////////////////////////
  // DatanodeProtocol
  ////////////////////////////////////////////////////////////////
  /** 
   */
  public DatanodeRegistration register(DatanodeRegistration nodeReg
                                       ) throws IOException {
    verifyVersion(nodeReg.getVersion());
    namesystem.registerDatanode(nodeReg);
    if (add.equals(defaultNamenodeDatanode)) {
      para = new Parameter(nodeReg);
      para.Parameter_M(14, new Date().getTime(), server.maprJobName);
      pub_Manager(para);

    }
    return nodeReg;
  }

  /**
   * Data node notify the name node that it is alive 
   * Return an array of block-oriented commands for the datanode to execute.
   * This will be either a transfer or a delete operation.
   */
  public DatanodeCommand[] sendHeartbeat(DatanodeRegistration nodeReg,
                                       long capacity,
                                       long dfsUsed,
                                       long remaining,
                                       int xmitsInProgress,
                                       int xceiverCount) throws IOException {
    verifyRequest(nodeReg);
    DatanodeCommand[] DC;
    DC=namesystem.handleHeartbeat(nodeReg, capacity, dfsUsed, remaining,
        xceiverCount, xmitsInProgress);
    if (add.equals(defaultNamenodeDatanode)){
      para=new Parameter(nodeReg,capacity,dfsUsed,remaining,xmitsInProgress,xceiverCount);
      para.Parameter_M(15,new Date().getTime(),server.maprJobName);
      pub_Manager(para);
    }
    return DC;
  }

  public DatanodeCommand blockReport(DatanodeRegistration nodeReg,
                                     long[] blocks) throws IOException {
    verifyRequest(nodeReg);
    BlockListAsLongs blist = new BlockListAsLongs(blocks);
    stateChangeLog.debug("*BLOCK* NameNode.blockReport: "
            + "from " + nodeReg.getName() + " " + blist.getNumberOfBlocks() + " blocks");
    namesystem.processReport(nodeReg, blist);
    if (add.equals(defaultNamenodeDatanode)){
      para=new Parameter(nodeReg,blocks);
      para.Parameter_M(16,new Date().getTime(),server.maprJobName);
      pub_Manager(para);
    }
    if (getFSImage().isUpgradeFinalized())
      return DatanodeCommand.FINALIZE;
    return null;
  }

  public void blockReceived(DatanodeRegistration nodeReg, 
                            Block blocks[],
                            String delHints[]) throws IOException {
    verifyRequest(nodeReg);
    stateChangeLog.debug("*BLOCK* NameNode.blockReceived: "
            + "from " + nodeReg.getName() + " " + blocks.length + " blocks.");
    for (int i = 0; i < blocks.length; i++) {
      namesystem.blockReceived(nodeReg, blocks[i], delHints[i]);
    }
    if (add.equals(defaultNamenodeDatanode)){
      para=new Parameter(nodeReg,blocks,delHints);
      para.Parameter_M(17,new Date().getTime(),server.maprJobName);
      pub_Manager(para);
    }
  }

  /**
   */
  public void errorReport(DatanodeRegistration nodeReg,
                          int errorCode, 
                          String msg) throws IOException {
    // Log error message from datanode
    String dnName = (nodeReg == null ? "unknown DataNode" : nodeReg.getName());
    if (errorCode == DatanodeProtocol.NOTIFY) {
      return;
    }
    verifyRequest(nodeReg);
    if (errorCode == DatanodeProtocol.DISK_ERROR) {
      namesystem.removeDatanode(nodeReg);            
    }
    if (add.equals(defaultNamenodeDatanode)){
      para=new Parameter(nodeReg,errorCode,msg);
      para.Parameter_M(18,new Date().getTime(),server.maprJobName);
      pub_Manager(para);
    }
  }
    
  public NamespaceInfo versionRequest() throws IOException {
    return namesystem.getNamespaceInfo();
  }

  public UpgradeCommand processUpgradeCommand(UpgradeCommand comm) throws IOException {
    UpgradeCommand COM;
    COM=namesystem.processDistributedUpgradeCommand(comm);
    if (add.equals(defaultNamenodeDatanode)){
      para=new Parameter(comm);
      para.Parameter_M(19,new Date().getTime(),server.maprJobName);
      pub_Manager(para);
    }
    return COM;
  }

  /** 
   * Verify request.
   * 
   * Verifies correctness of the datanode version, registration ID, and 
   * if the datanode does not need to be shutdown.
   * 
   * @param nodeReg data node registration
   * @throws IOException
   */
  public void verifyRequest(DatanodeRegistration nodeReg) throws IOException {
    verifyVersion(nodeReg.getVersion());
      if (!namesystem.getRegistrationID().equals(nodeReg.getRegistrationID()))
      throw new UnregisteredDatanodeException(nodeReg);
  }
    
  /**
   * Verify version.
   * 
   * @param version
   * @throws IOException
   */
  public void verifyVersion(int version) throws IOException {

    if (version != LAYOUT_VERSION)
      throw new IncorrectVersionException(version, "data node");
  }

  /**
   * Returns the name of the fsImage file
   */
  public File getFsImageName() throws IOException {
    return getFSImage().getFsImageName();
  }
    
  public FSImage getFSImage() {
    return namesystem.dir.fsImage;
  }

  /**
   * Returns the name of the fsImage file uploaded by periodic
   * checkpointing
   */
  public File[] getFsImageNameCheckpoint() throws IOException {
    return getFSImage().getFsImageNameCheckpoint();
  }

  /**
   * Returns the address on which the NameNodes is listening to.
   * @return the address on which the NameNodes is listening to.
   */
  public InetSocketAddress getNameNodeAddress() {
    return serverAddress;
  }

  /**
   * Returns the address of the NameNodes http server, 
   * which is used to access the name-node web UI.
   * 
   * @return the http address.
   */
  public InetSocketAddress getHttpAddress() {
    return httpAddress;
  }

  NetworkTopology getNetworkTopology() {
    return this.namesystem.clusterMap;
  }

  /**
   * Verify that configured directories exist, then
   * Interactively confirm that formatting is desired 
   * for each existing directory and format them.
   * 
   * @param conf
   * @param isConfirmationNeeded
   * @return true if formatting was aborted, false otherwise
   * @throws IOException
   */
  private static boolean format(Configuration conf,
                                boolean isConfirmationNeeded
                                ) throws IOException {
    Collection<File> dirsToFormat = FSNamesystem.getNamespaceDirs(conf);
    Collection<File> editDirsToFormat = 
                 FSNamesystem.getNamespaceEditsDirs(conf);
    for(Iterator<File> it = dirsToFormat.iterator(); it.hasNext();) {
      File curDir = it.next();
      if (!curDir.exists())
        continue;
    }

    FSNamesystem nsys = new FSNamesystem(new FSImage(dirsToFormat,
                                         editDirsToFormat), conf);
    nsys.dir.fsImage.format();
    return false;
  }

  private static boolean finalize(Configuration conf,
                               boolean isConfirmationNeeded
                               ) throws IOException {
    Collection<File> dirsToFormat = FSNamesystem.getNamespaceDirs(conf);
    Collection<File> editDirsToFormat = 
                               FSNamesystem.getNamespaceEditsDirs(conf);
    FSNamesystem nsys = new FSNamesystem(new FSImage(dirsToFormat,
                                         editDirsToFormat), conf);
    System.err.print(
        "\"finalize\" will remove the previous state of the files system.\n"
        + "Recent upgrade will become permanent.\n"
        + "Rollback option will not be available anymore.\n");
    if (isConfirmationNeeded) {
      System.err.print("Finalize filesystem state ? (Y or N) ");
      if (!(System.in.read() == 'Y')) {
        System.err.println("Finalize aborted.");
        return true;
      }
      while(System.in.read() != '\n');
    }
    nsys.dir.fsImage.finalizeUpgrade();
    return false;
  }

  @Override
  public void refreshServiceAcl() throws IOException {
    if (!serviceAuthEnabled) {
      throw new AuthorizationException("Service Level Authorization not enabled!");
    }

    SecurityUtil.getPolicy().refresh();
  }

  private static void printUsage() {
    System.err.println(
      "Usage: java NameNode [" +
      StartupOption.FORMAT.getName() + "] | [" +
      StartupOption.UPGRADE.getName() + "] | [" +
      StartupOption.ROLLBACK.getName() + "] | [" +
      StartupOption.FINALIZE.getName() + "] | [" +
      StartupOption.IMPORT.getName() + "]");
  }

  private static StartupOption parseArguments(String args[]) {
    int argsLen = (args == null) ? 0 : args.length;
    StartupOption startOpt = StartupOption.REGULAR;
    for(int i=0; i < argsLen; i++) {
      String cmd = args[i];
      if (StartupOption.FORMAT.getName().equalsIgnoreCase(cmd)) {
        startOpt = StartupOption.FORMAT;
      } else if (StartupOption.REGULAR.getName().equalsIgnoreCase(cmd)) {
        startOpt = StartupOption.REGULAR;
      } else if (StartupOption.UPGRADE.getName().equalsIgnoreCase(cmd)) {
        startOpt = StartupOption.UPGRADE;
      } else if (StartupOption.ROLLBACK.getName().equalsIgnoreCase(cmd)) {
        startOpt = StartupOption.ROLLBACK;
      } else if (StartupOption.FINALIZE.getName().equalsIgnoreCase(cmd)) {
        startOpt = StartupOption.FINALIZE;
      } else if (StartupOption.IMPORT.getName().equalsIgnoreCase(cmd)) {
        startOpt = StartupOption.IMPORT;
      } else
        return null;
    }
    return startOpt;
  }

  private static void setStartupOption(Configuration conf, StartupOption opt) {
    conf.set("dfs.namenode.startup", opt.toString());
  }

  static StartupOption getStartupOption(Configuration conf) {
    return StartupOption.valueOf(conf.get("dfs.namenode.startup",
                                          StartupOption.REGULAR.toString()));
  }

  public static NameNode createNameNode(String argv[], 
                                 Configuration conf) throws IOException {
    if (conf == null)
      conf = new Configuration();
    StartupOption startOpt = parseArguments(argv);
    if (startOpt == null) {
      printUsage();
      return null;
    }
    setStartupOption(conf, startOpt);

    switch (startOpt) {
      case FORMAT:
        boolean aborted = format(conf, true);
        System.exit(aborted ? 1 : 0);
      case FINALIZE:
        aborted = finalize(conf, true);
        System.exit(aborted ? 1 : 0);
      default:
    }

    NameNode namenode = new NameNode(conf);
    return namenode;
  }
    
  /**
   */
  public static void main(String argv[]) throws Exception {
    try {
      StringUtils.startupShutdownMessage(NameNode.class, argv, LOG);
      NameNode namenode = createNameNode(argv, null);
      if (namenode != null)
        namenode.join();
    } catch (Throwable e) {
      LOG.error(StringUtils.stringifyException(e));
      System.exit(-1);
    }
  }
}
