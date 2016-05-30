package main;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.TreeMap;

/**
 * Created by kuozhang on 4/22/16.
 */
public class Run {

    public static void main(String [] args) throws IOException {
        /**
         *  Top K value
         */
        int k=4;
        /**
         * data file name
         */
        String fileName= new String("/Users/kuozhang/Documents/Buffalo/2016/Course/CSE 562/TestDataSet1");
        /**
         * weight
         * @Warning the dimension should be equal to the number of attribute
         */
        Float[] weight={new Float(1),new Float(1),new Float(1.5)};

        /*************begin execution*********************************************/
        /*************begin execution*********************************************/
        /*************begin execution*********************************************/

        /**
         * load data input loadDataTable class instance
         */
        LoadDataTable loadDataTable=new LoadDataTable(fileName);
        /**
         * construct a b-tree on ID, return its reference
         */
        //System.out.println(loadDataTable.getTable());
        Index index=new Index();
        index.createBtreeID(loadDataTable.getTable());
        ArrayList<Float> _weight=new ArrayList<Float>(Arrays.asList(weight));
        /**
         * create a Algorithm instance
         */
        Algorithm alg=new Algorithm(loadDataTable.getTable(),index.getBtreeOnID(),k,loadDataTable.getTable().get(0).size()-1);
        /**
         * build secondary dense Btree
         */
        //alg.buildSecondaryBtree();
        //ArrayList<BTree<Float,ArrayList<Float>>> denseSecondaryBt=alg.getSecondaryBtreeList();//get dense secondary btree
        //System.out.println(denseSecondaryBt.get(0).get(new Float(2)));
        //System.exit(0);


        TreeMap<Float,ArrayList<Float>> res1=alg.runNaiveAlgorithmNew(_weight);
        TreeMap<Float,ArrayList<Float>> res2=alg.runThresholdAlgUseSecondaryBtree(_weight);
        alg.printResultForNaiveAlg(res1);
        alg.printResultTA(res2);
       /* Integer a=new Integer(1);
        ArrayList<Integer> c =new ArrayList<Integer>(a);
        System.out.println(a+5);*/
    }
}
