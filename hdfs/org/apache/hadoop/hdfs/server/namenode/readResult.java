package org.apache.hadoop.hdfs.server.namenode;

public class readResult{
    String src;
    int result;
    readResult(String src,int result){
        this.src=src;
        this.result=result;

    }
}