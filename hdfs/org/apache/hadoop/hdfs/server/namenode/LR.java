package org.apache.hadoop.hdfs.server.namenode;


import Jama.Matrix;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.util.ArrayList;


public class LR {
    public static final Log LOG = LogFactory.getLog(NameNode.class.getName());
    public static final Log stateChangeLog = LogFactory.getLog("org.apache.hadoop.hdfs.StateChange");
    double[][] weight ={{0.5},{0.5},{0.5}};
    Matrix weightMat = new Matrix(weight);

    /**
     * 获取特征数据
     */
    public Matrix getDataMat(ArrayList<testSet> M) {
        try {

            double[][] data = new double[M.size()][3];
            for (int j = 0; j < M.size(); j++) {
                data[j][0] = M.get(j).read_fre;
                data[j][1] = M.get(j).write_fre;
                data[j][2] = M.get(j).time_dis;
            }

            Matrix dataMatrix = new Matrix(data);
            return dataMatrix;
        } catch (Exception e) {
            e.printStackTrace();
            return null;
        }
    }

    /**
     * 获取标签数据
     */
    public Matrix getLabelMat(ArrayList<testSet> M) {
        try {

            double[][] label = new double[M.size()][1];

            for (int i = 0; i < M.size(); i++) {
                label[i][0] = M.get(i).result;
            }

            Matrix labelMatrix = new Matrix(label);

            return labelMatrix;
        } catch (Exception e) {
            e.printStackTrace();
            return null;
        }
    }

    /**
     * sigmoid函数
     */
    public Matrix sigmoid(Matrix intX) {
        double[][] tmp = new double[intX.getRowDimension()][intX.getColumnDimension()];
        for (int i = 0; i < intX.getRowDimension(); i++) {
            tmp[i][0] = 1.0 / (1 + Math.exp(intX.get(i, 0)));
        }
        Matrix tmpMat = new Matrix(tmp);
        return tmpMat;
    }


    public void analyze(Matrix dataMat, Matrix labelMat) {



        //步长
        double alpha = 0.001;
        //迭代次数
        int maxCycles = 500;
        //weights权重列向量

        //X*weight
        Matrix mm;
        Matrix h;
        Matrix e;

        //
        for (int i = 1; i < maxCycles; i++) {
            mm = dataMat.times(weightMat).times(-1);
            h = sigmoid(mm);
            e = labelMat.minus(h);
            weightMat = weightMat.plus(dataMat.transpose().times(e).times(alpha));


            //for (int j = 0; j < weightMat.getRowDimension(); j++) {
              //  System.out.println(weightMat.get(j, 0));
            //}
        }
        System.out.println("--------------------------------" + "**" + "----------------------------------------------------------");
    }

    public double test(Matrix dataMat) {
        Matrix mm = dataMat.times(weightMat).times(-1);
        Matrix h = sigmoid(mm);
        for (int j = 0; j < h.getRowDimension(); j++) {
            System.out.println(h.get(j, 0));}
        return h.get(0,0);
    }

}




