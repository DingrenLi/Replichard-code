package org.apache.hadoop.hdfs.server.namenode;

import org.apache.hadoop.io.Text;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class srcMessage{
    String[] src;
    byte[] mTree;
    Parameter[] parameters;
    String erro;
    srcMessage(String[] src,byte[] mTree,Parameter[] parameters,String erro){
        this.src=src;
        this.mTree=mTree;
        this.parameters=parameters;
        this.erro=erro;
    }
    srcMessage(){}
    public void write(DataOutput output) throws IOException{
        if (src!=null){
                        output.writeBoolean(true);
                        output.writeInt(src.length);
                        for(int i=0;i<src.length;i++){
                            Text.writeString(output, (src[i] == null) ? "" : src[i]);}
                    }else output.writeBoolean(false);
        if (mTree==null){ output.writeBoolean(false);
        }else {
            output.writeBoolean(true);
        output.write(mTree);
        }
        if (parameters==null){output.writeBoolean(false);}
        else {
            output.writeBoolean(true);
            output.writeInt(parameters.length);
            for(int i=0;i<parameters.length;i++) {
            parameters[i].write(output);
            }
            if (erro==null)output.writeBoolean(false);
            else {
                output.writeBoolean(true);
                Text.writeString(output, erro);}
            }
    }
   public  void readFields(DataInput input)throws IOException {
       Boolean gnamePresent=input.readBoolean();
       if (gnamePresent){
           int len=input.readInt();
           src=new String[len];
           for (int j=0;j<len;j++){
               src[j]=Text.readString(input);
               if (src[j].isEmpty()){
                   src[j]=null;
               }}
       }else src=null;
       Boolean mtreePresent=input.readBoolean();
       if (mtreePresent){
       input.readFully(mTree);}
       else mTree=null;

       Boolean paraPresent=input.readBoolean();
       if (paraPresent){
           int len=input.readInt();
           parameters=new Parameter[len];
           for (int j=0;j<len;j++){
               parameters[j].readFields(input);
           }}else parameters=null;
       Boolean erroPresent=input.readBoolean();
       if (erroPresent){
           erro=Text.readString(input);
       }
   }


}