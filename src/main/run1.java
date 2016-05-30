package main;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.TreeMap;

/**
 * Created by kuozhang on 4/23/16.
 * use XinMa's ReadCSV class
 */
public class run1 {
    public static void main(String [] args) throws IOException {
        /**
         *  Top K value
         */
        int k=5;
        /**
         * data file name
         */
        String fileName= new String("/Users/kuozhang/Dropbox/DataBaseProjectForShare/XinMa/TopKQuery/src/code/NBA.csv");
        /**
         * weight
         * @Warning the dimension should be equal to the number of attribute
         */
        Float[] weight={new Float(1),new Float(1.9),new Float(4.5),new Float(4.5),new Float(4.5),new Float(4.5),new Float(4.5),new Float(45),new Float(4.5),new Float(4.5),new Float(4.5),new Float(4.5),new Float(4.5),new Float(4.5),new Float(4.5),new Float(4.5),new Float(4.5)};
        /**
         * dataTable
         */
        ArrayList<ArrayList<Float>> dataTable;

        /*************begin execution*********************************************/
        /*************begin execution*********************************************/
        /*************begin execution*********************************************/
        /**
         * instantiate a ReadCSV class which is created by XIN MA
         */
        ReadCSV readCSV=new ReadCSV();
        dataTable=readCSV.getTable(fileName);
        /**
         * construct a b-tree on ID, return its reference
         */
        Index index=new Index();
        index.createBtreeID(dataTable);
        ArrayList<Float> _weight=new ArrayList<Float>(Arrays.asList(weight));
        /**
         * create a Algorithm instance
         */
        Algorithm alg=new Algorithm(dataTable,index.getBtreeOnID(),k,dataTable.get(0).size()-1);
        TreeMap<Float,ArrayList<Float>> res1=alg.runNaiveAlgorithmNew(_weight);
        TreeMap<Float,ArrayList<Float>> res2=alg.runThresholdAlgUseSecondaryBtree(_weight);
        alg.printResultForNaiveAlg(res1);
        alg.printResultTA(res2);

    }
}
