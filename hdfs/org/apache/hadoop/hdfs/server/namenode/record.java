package org.apache.hadoop.hdfs.server.namenode;
public class record{
    int read;
    String src;
    long time;
    record(int read,String src,long time){
        this.read=read;
        this.src=src;
        this.time=time;
    }
}