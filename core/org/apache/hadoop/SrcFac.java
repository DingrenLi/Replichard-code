package org.apache.hadoop;
public  class SrcFac{
    SrcFac(){};
    public static String srcfac(String src){
        if (src.contains("/home/hadoop/tmp/mapred/system")){
            src="/home/hadoop/tmp/mapred/system";
        } else if (src.contains("/data/grepp")){
            src="/data/grepp";
        }else if (src.contains("/data/rankings")){
            src="/data/rankings";
        } else if (src.contains("/data/uservisits")) {
            src = "/data/uservisits";
        }else if (src.contains("/user/hadoop/output/pig_bench/grep_select")){
            src="/user/hadoop/output/pig_bench/grep_select";
        }else if (src.contains("/user/hadoop/output/pig_bench/rankings_select")){
            src="/user/hadoop/output/pig_bench/rankings_select";
        } else if (src.contains("/user/hadoop/output/pig_bench/html_join")){
            src="/user/hadoop/output/pig_bench/html_join";
        }else if (src.contains("/user/hadoop/output/pig_bench/uservisits_aggre")){
            src="/user/hadoop/output/pig_bench/uservisits_aggre";
        }else if (src.contains("/tmp")){
            src="/tmp";
        }else {
            System.out.println("XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX" +
                    "VVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVV" +
                    "FFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF");
        }

        return src;
    }
}