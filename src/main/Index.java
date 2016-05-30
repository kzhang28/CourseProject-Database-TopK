package main;

import java.io.IOException;
import java.util.ArrayList;
import java.util.TreeMap;

/**
 * Created by kuozhang on 4/18/16.
 * This class implements B-Tree index and other necessary data structures for Top k query
 */
public class Index {
    /**
     * the Btree index on id
     */
    private BTree<Float,ArrayList<Float>> bTreeOnID=new BTree<Float,ArrayList<Float>>();
    /**
     * A list that holds all the Btree indices build on each individual attributes of a record.
     */
    private ArrayList<BTree> bTreeList;
    /**
     * create the Btree index on ID
     * @param arrayList should be a N*N ArrayList<ArrayList>, The first column should be id
     * @return true if successfully created a BTree (initialize field: bTreeOnID)
     */
    public boolean createBtreeID(ArrayList<ArrayList<Float>> arrayList){
        for(ArrayList<Float> i: arrayList){
            bTreeOnID.put(i.get(0),i);
        }
        //System.out.println("Information: Btree index on id is build successfully!");
        return true;
    }
    public BTree<Float,ArrayList<Float>> getBtreeOnID(){
        return bTreeOnID;
    }

}
