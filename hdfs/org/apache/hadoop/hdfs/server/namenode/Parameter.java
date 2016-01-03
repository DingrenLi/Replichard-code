package org.apache.hadoop.hdfs.server.namenode;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.hdfs.server.protocol.DatanodeRegistration;
import org.apache.hadoop.hdfs.server.protocol.UpgradeCommand;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

;

public class Parameter implements Writable{

    public static final Log LOG = LogFactory.getLog(NameNode.class.getName());
    public static final Log stateChangeLog = LogFactory.getLog("org.apache.hadoop.hdfs.StateChange");

    String src;
    String dclientName;
    boolean overwrite;
    short replication;
    long blockSize;
    FsPermission mpermission;
    String groupname;
    Block b=new Block();
    String holder;
    boolean recursive;
    int  methodname;
    long  time;
    String jobname;
    String username;
    String user;
    String[] gname;

    DatanodeRegistration nodeReg=new DatanodeRegistration();
    long capacity;
    long dfsUsed;
    long remaining;
    int xmitsInProgress;
    int xceiverCount;
    long[] blocks;
    String[] delHints;
    Block[] block;
    int errorCode;
    String msg;
    UpgradeCommand comm=new UpgradeCommand();
    LocatedBlock[] bloc;

    Parameter(String src,String clientname,Block b){
        this.src=src;
        this.dclientName=clientname;
        this.b=b;
    }
   Parameter(String src, FsPermission masked, String clientName, boolean overwrite, short replication, long blockSize,String user,String[] gname){
       this.src=src;
       this.mpermission=masked;
       this.dclientName=clientName;
       this.overwrite=overwrite;
       this.replication=replication;
       this.blockSize=blockSize;
       this.user=user;
       this.gname=gname;
   }
    Parameter(String src, String clientName){
        this.src=src;
        this.dclientName=clientName;
    }
    Parameter(String src, short replication){
        this.src=src;
        this.replication=replication;
    }
    Parameter(String src, FsPermission permission){
        this.src=src;
        this.mpermission=permission;

    }
    Parameter(String src, FsPermission permission,String user,String[] groupname){
        this.src=src;
        this.mpermission=permission;
        this.user=user;
        this.gname=groupname;
    }
    Parameter(String src, String username, String groupname){
        this.src=src;
        this.username=username;
        this.groupname=groupname;
    }
    Parameter(Block b, String src, String holder){
        this.b=b;
        this.src=src;
        this.holder=holder;
    }
   Parameter(String src){
       this.src=src;
   }
    Parameter(String src, boolean recursive,String usr,String[]gname){
        this.src=src;
        this.recursive=recursive;
        this.user=usr;

        this.gname=gname;
    }
   Parameter(DatanodeRegistration nodeReg){this.nodeReg=nodeReg;}
    Parameter(DatanodeRegistration nodeReg,
              long capacity,
              long dfsUsed,
              long remaining,
              int xmitsInProgress,
              int xceiverCount){
        this.nodeReg=nodeReg;
        this.capacity=capacity;
        this.dfsUsed=dfsUsed;
        this.remaining=remaining;
        this.xmitsInProgress=xmitsInProgress;
        this.xceiverCount=xceiverCount;}
    Parameter(DatanodeRegistration nodeReg,
              long[] blocks){
        this.nodeReg=nodeReg;
        this.blocks=blocks;
    }
    Parameter(DatanodeRegistration nodeReg,
              Block blocks[], String delHints[]){
        this.nodeReg=nodeReg;
        this.block=blocks;

        this.delHints=delHints;
    }
    Parameter(DatanodeRegistration nodeReg,int errorCode,
               String msg){
        this.nodeReg=nodeReg;
        this.errorCode=errorCode;
        this.msg=msg;
    }
    Parameter(UpgradeCommand comm){this.comm=comm;}
    Parameter(LocatedBlock[] blocks){this.bloc=blocks;}
    Parameter(){}


    public void Parameter_M(int methodname, long time, String jobname)throws IOException{
        this.methodname=methodname;
        this.time=time;
        this.jobname=jobname;}

    public void write(DataOutput out) throws IOException {
        out.writeInt(methodname);
       out.writeLong(time);
        Text.writeString(out,(jobname==null)?"":jobname);
        Text.writeString(out,(src==null)?"":src);
        switch (methodname){
            case 1:{
                   if (mpermission!=null){
                       out.writeBoolean(true);
                       out.writeShort(mpermission.toShort());
                   } else out.writeBoolean(false);
                   Text.writeString(out,(dclientName== null) ? "" : dclientName);
                   out.writeBoolean(overwrite);
                   out.writeShort(replication);
                   out.writeLong(blockSize);
                   if (user==null) LOG.info("AAAAAAAAAAAAAAAAAAAANuLL");
                   Text.writeString(out, (user == null) ? "" : user);
                    if (gname!=null){
                        out.writeBoolean(true);
                        out.writeInt(gname.length);
                        for(int i=0;i<gname.length;i++){
                            Text.writeString(out,(gname[i]==null)?"":gname[i]);}
                    }else out.writeBoolean(false);
                break;}
            case 2:{Text.writeString(out,(dclientName== null) ? "" : dclientName);
                break;}
            case 3:{out.writeShort(replication);
                break;}
            case 4:{
                if (mpermission!=null){
                    out.writeBoolean(true);
                    out.writeShort(mpermission.toShort());
                } else out.writeBoolean(false);
                break;}
            case 5:{Text.writeString(out,(username== null) ? "" : username);
                   Text.writeString(out,(groupname== null) ? "" : groupname);
                break;}
            case 6:{if (b!=null){
                out.writeBoolean(true);
                b.write(out);
                }else out.writeBoolean(false);
                Text.writeString(out,(holder== null) ? "" : holder);
                break;}
            case 7:{if (b!=null){
                out.writeBoolean(true);
                b.write(out);
                }else out.writeBoolean(false);
                Text.writeString(out,(dclientName== null) ? "" : dclientName);
                break;}
            case 8:{Text.writeString(out,(dclientName== null) ? "" : dclientName);
                break;}
            case 9:{Text.writeString(out,(dclientName== null) ? "" : dclientName);
                break;}
            case 10:{break;}
            case 11:{out.writeBoolean(recursive);
                Text.writeString(out, (user == null) ? "" : user);
                if (gname!=null){
                    out.writeBoolean(true);
                    out.writeInt(gname.length);
                    for(int i=0;i<gname.length;i++){
                        Text.writeString(out,(gname[i]==null)?"":gname[i]);}
                }else out.writeBoolean(false);
                break;}

            case 12:{
                if (mpermission!=null){
                out.writeBoolean(true);
                out.writeShort(mpermission.toShort());
                } else out.writeBoolean(false);
                Text.writeString(out, (user == null) ? "" : user);
                if (gname!=null){
                    out.writeBoolean(true);
                    out.writeInt(gname.length);
                    for(int i=0;i<gname.length;i++){
                        Text.writeString(out,(gname[i]==null)?"":gname[i]);}
                }else out.writeBoolean(false);
                break;}
            case 13:{break;}
            case 14:{
                if (nodeReg!=null){
                    out.writeBoolean(true);
                    nodeReg.write(out);
                }else out.writeBoolean(false);
                break;}
            case 15:{
                if (nodeReg!=null){
                    out.writeBoolean(true);
                    nodeReg.write(out);
                }else out.writeBoolean(false);
                out.writeLong(capacity);
                     out.writeLong(dfsUsed);
                     out.writeLong(remaining);
                     out.writeInt(xmitsInProgress);
                     out.writeInt(xceiverCount);
                    break;}

            case 16:{
                if (nodeReg!=null){
                    out.writeBoolean(true);
                    nodeReg.write(out);
                }else out.writeBoolean(false);
                if (blocks!=null){
                    out.writeBoolean(true);
                int len=blocks.length;
                out.writeInt(len);
                for (int j=0;j<len;j++){
                          out.writeLong(blocks[j]);}
                      break;
                }else out.writeBoolean(false);
            }

            case 17:{
                if (nodeReg!=null){
                    out.writeBoolean(true);
                    nodeReg.write(out);
                }else out.writeBoolean(false);
                if (block!=null){
                    out.writeBoolean(true);
                int len=block.length;
                     out.writeInt(len);
                      for (int j=0;j<len;j++){
                          block[j].write(out);
                      }}else out.writeBoolean(false);
                if (delHints!=null){
                      out.writeBoolean(true);
                      int len=delHints.length;
                      out.writeInt(len);
                      for (int j=0;j<len;j++){
                          Text.writeString(out, (delHints[j] == null) ? "" : delHints[j]);
                      }}else out.writeBoolean(false);
                    break;}

            case 18:{
                if (nodeReg!=null){
                    out.writeBoolean(true);
                    nodeReg.write(out);
                }else out.writeBoolean(false);
                out.writeInt(errorCode);
                Text.writeString(out,(msg==null)?"":msg);
                break;}
            case 19:{if (comm!=null){
                out.writeBoolean(true);
                comm.write(out);
            }else out.writeBoolean(false);
                break;}

            case 20:{if (bloc!=null){
                         out.writeBoolean(true);
                     int len=bloc.length;
                      out.writeInt(len);
                      for (int j=0;j<len;j++){
                          bloc[j].write(out);
                      }}else out.writeBoolean(false);
                     break;}}}


   public void readFields(DataInput in)throws IOException{

       methodname=in.readInt();
       time=in.readLong();
       jobname=Text.readString(in);
       if (jobname.isEmpty()){jobname=null;}
       src=Text.readString(in);
       if (src.isEmpty()){src=null;}
     //  LOG.info("methodname is"+methodname);
       switch (methodname) {
           case 1:{
               Boolean permissionPresent=in.readBoolean();
               if (permissionPresent){
                   mpermission=new FsPermission(in.readShort());
               }else mpermission=null;
               dclientName=Text.readString(in);
               if (dclientName.isEmpty()){dclientName=null;}
               overwrite=in.readBoolean();
               replication=in.readShort();
               blockSize=in.readLong();
               user=Text.readString(in);
               if(user.isEmpty()){user=null;}
               if (user==null) LOG.info("BBBBBBBBBBBBBBBBBNuLL");
               Boolean gnamePresent=in.readBoolean();
               if (gnamePresent){
                   int len=in.readInt();
                   gname=new String[len];
                   for (int j=0;j<len;j++){
                       gname[j]=Text.readString(in);
                       if (gname[j].isEmpty()){
                           gname[j]=null;
                           groupname=null;}}
               }else gname=null;
               break;}
               /*create*/
            /*String src, FsPermission masked, String clientName,boolean overwrite, short replication, long blockSize*/
           case 2:{dclientName=Text.readString(in);
                  if (dclientName.isEmpty()){dclientName=null;}
                   break;}
              /* public LocatedBlock append(String src, String clientName) throws IOException;*/

           case 3:{replication=in.readShort();
                    break;}
              /* public boolean setReplication(String src,short replication) throws IOException;*/

           case 4:{
               Boolean permissionPresent=in.readBoolean();
               if (permissionPresent){
                   mpermission=new FsPermission(in.readShort());
               }else mpermission=null;
                   break;}
               /* public void setPermission(String src, FsPermission permission
      ) throws IOException;
*/
           case 5:{username=Text.readString(in);
                  groupname=Text.readString(in);
                  if (groupname.isEmpty()){groupname=null;}
                  if (username.isEmpty()){username=null;}
                   break;}
           /* public void setOwner(String src, String username, String groupname
      ) throws IOException;*/
           case 6:{Boolean blockPresent=in.readBoolean();
                   if (blockPresent)b.readFields(in);
                   else b=null;
                  holder=Text.readString(in);
                  if (holder.isEmpty()){holder=null;}
                   break;}
               /* public void abandonBlock(Block b, String src, String holder
      ) throws IOException;*/
           case 7:{Boolean blockPresent=in.readBoolean();
                  if (blockPresent)b.readFields(in);
                  else b=null;
                   dclientName=Text.readString(in);
                  if (dclientName.isEmpty()){dclientName=null;}
                   break;}
               /*public LocatedBlock addBlock(String src, String clientName) throws IOException;*/
           case 8:{dclientName=Text.readString(in);
                  if(dclientName.isEmpty()){dclientName=null;}
                   break;}
               /*public boolean complete(String src, String clientName) throws IOException;*/
           case 9:{dclientName=Text.readString(in);
                  if (dclientName.isEmpty()){dclientName=null;}
                  break;}
               /*public boolean rename(String src, String dst) throws IOException;*/

           case 10:{break;}/*public boolean delete(String src) throws IOException;*/

           case 11:{recursive=in.readBoolean();
               user=Text.readString(in);
               if(user.isEmpty()){user=null;}
               Boolean gnamePresent=in.readBoolean();
               if (gnamePresent){
                   int len=in.readInt();
                   gname=new String[len];
                   for (int j=0;j<len;j++){
                       gname[j]=Text.readString(in);
                       if (gname[j].isEmpty()){
                           gname[j]=null;
                           groupname=null;}}
               }else gname=null;
                    break;}
           /* public boolean delete(String src, boolean recursive) throws IOException;*/
           case 12: { Boolean permissionPresent=in.readBoolean();
               if (permissionPresent){
                   mpermission=new FsPermission(in.readShort());
               }else mpermission=null;
                     user=Text.readString(in);
                     if(user.isEmpty()){user=null;}
                 Boolean gnamePresent=in.readBoolean();
                 if (gnamePresent){
                   int len=in.readInt();
                   gname=new String[len];
                   for (int j=0;j<len;j++){
                       gname[j]=Text.readString(in);
                       if (gname[j].isEmpty()){
                           gname[j]=null;
                           groupname=null;}}
               }else gname=null;

                         /* public boolean mkdirs(String src, FsPermission masked) throws IOException;*/
                     break;}
           case 13:{break;}
                   /*renewLease*/
            case 14: {Boolean nodeRegPresent=in.readBoolean();
                      if (nodeRegPresent){
                      nodeReg.readFields(in);
                      }else nodeReg=null;
                      break;}
                   /*public DatanodeRegistration register(DatanodeRegistration registration
                                       ) throws IOException;*/
           case 15:{
               Boolean nodeRegPresent=in.readBoolean();
               if (nodeRegPresent){
                   nodeReg.readFields(in);
               }else nodeReg=null;
                   capacity=in.readLong();
                   dfsUsed=in.readLong();
                   remaining=in.readLong();
                   xmitsInProgress=in.readInt();
                   xceiverCount=in.readInt();
                   break;}
                   /*public DatanodeCommand[] sendHeartbeat(DatanodeRegistration registration,
                                       long capacity,
                                       long dfsUsed, long remaining,
                                       int xmitsInProgress,
                                       int xceiverCount) throws IOException;*/
           case 16:{Boolean nodeRegPresent=in.readBoolean();
               if (nodeRegPresent){
                   nodeReg.readFields(in);
               }else nodeReg=null;
                   Boolean blocksPresent=in.readBoolean();
                   if (blocksPresent){
                   int len=in.readInt();
                     blocks=new long[len];
                   for (int j=0;j<len;j++){
                       blocks[j]=in.readLong();
                       }}else blocks=null;
               break;}
                   /*public DatanodeCommand blockReport(DatanodeRegistration registration,
                                     long[] blocks) throws IOException;*/
           case 17:{Boolean nodeRegPresent=in.readBoolean();
               if (nodeRegPresent){
                   nodeReg.readFields(in);
               }else nodeReg=null;
                   Boolean blockPresent=in.readBoolean();
                   if (blockPresent){
                   int len=in.readInt();
                   block=new Block[len];
                  for (int j=0;j<len;j++){
                      Block c=new Block();
                      c.readFields(in);
                      block[j]=c;
                     }}else block=null;
                   Boolean delHinsPresent=in.readBoolean();
                   if (delHinsPresent) {
                       int len = in.readInt();
                       delHints = new String[len];
                       for (int j = 0; j < len; j++) {
                           delHints[j] = Text.readString(in);
                           if (delHints[j].isEmpty()) {
                               delHints[j] = null;
                           }
                       }
                   }else delHints=null;
                   break;}
           /*public void blockReceived(DatanodeRegistration registration,
                            Block blocks[],
                            String[] delHints) throws IOException;*/
           case 18: {
               Boolean nodeRegPresent=in.readBoolean();
               if (nodeRegPresent){
                   nodeReg.readFields(in);
               }else nodeReg=null;
               errorCode = in.readInt();
               msg = Text.readString(in);
               if (msg.isEmpty()) {
                   msg = null;
               }
               break;
           }
           /* public void errorReport(DatanodeRegistration registration,
                          int errorCode,
                          String msg) throws IOException;*/
           case 19:{
               Boolean commPresent=in.readBoolean();
               if (commPresent){
                   comm.readFields(in);
               }else comm=null;
                   break;}
            /*UpgradeCommand processUpgradeCommand(UpgradeCommand comm) throws IOException;*/
           case 20: {
               Boolean blocPresent=in.readBoolean();
               if (blocPresent){
               int len=in.readInt();
                      bloc=new LocatedBlock[len];
                     for (int j=0;j<len;j++){
                   bloc[j].readFields(in);}
               }else bloc=null;
                   break;}
           /*public void reportBadBlocks(LocatedBlock[] blocks) throws IOException;*/


           }
       }
    public String get_src(){return src;}
    public String get_clientname_dist(){ return dclientName;}
    public boolean get_overwrite(){return overwrite;}
    public short get_replication(){return replication;}
    public long get_blocksize(){return blockSize;}
    public FsPermission get_mask_permmition(){return mpermission;}
    public String get_groupname(){return groupname;}
    public Block get_Block(){return b;}
    public String get_holder(){return holder;}
    public boolean get_recursive(){return recursive;}
    public int get_methodname(){return methodname;}
    public long get_time(){return time;}
    public String get_jobname(){return jobname;}
    public String get_usrname(){return username;}
    public String get_user(){return user;}
    public String[] get_Gname(){return gname;}
    public DatanodeRegistration get_nodeReg(){return nodeReg;}
    public long get_capacity(){return capacity;}
    public long get_dfsUsed(){return dfsUsed;}
    public long get_remaining(){return remaining;}
    public int get_xmitsInProgress(){return xmitsInProgress;}
    public int get_xceiverCount(){return xceiverCount;}
    public long[] get_blocks(){return blocks;}
    public String[] get_delHints(){return delHints;}
    public Block[] get_block(){
        return block;}
    public int get_errorCode(){return errorCode;}
    public String get_msg(){return msg;}
    public UpgradeCommand get_comm(){return comm;}
    public LocatedBlock[] get_bloc(){return bloc;}

}