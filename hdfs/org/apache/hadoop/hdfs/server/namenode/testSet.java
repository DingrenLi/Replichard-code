package org.apache.hadoop.hdfs.server.namenode;
public class testSet{
    int read_fre;
    int write_fre;
    long time_dis;
    int result;
    testSet(int read_fre,int write_fre,long time_dis,int result){
        this.read_fre=read_fre;
        this.write_fre=write_fre;
        this.time_dis=time_dis;
        this.result=result;
    }

}