package main;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.Arrays;

/**
 * Created by kuozhang on 4/26/16.
 */
public class RunJoin {
    public static void main(String [] args) throws IOException {
        String f1="/Users/kuozhang/Documents/Buffalo/2016/Course/CSE 562/TestData1";
        String f2="/Users/kuozhang/Documents/Buffalo/2016/Course/CSE 562/TestData2";
        String f3="/Users/kuozhang/Documents/Buffalo/2016/Course/CSE 562/TestData3";
        int k=2;
        int N=6;
        /*
        ReadCSV readCSV1=new ReadCSV();
        ReadCSV readCSV2=new ReadCSV();
        ArrayList<ArrayList<Float>> t1=readCSV1.getTable(f1);
        ArrayList<ArrayList<Float>> t2=readCSV2.getTable(f2);*/

        LoadDataTable l=new LoadDataTable(f1);
        LoadDataTable l2=new LoadDataTable(f2);
        LoadDataTable l3=new LoadDataTable(f3);

        ArrayList<ArrayList<Float>> t1=l.getTable();
        ArrayList<ArrayList<Float>> t2=l2.getTable();
        ArrayList<ArrayList<Float>> t3=l3.getTable();

        ArrayList<ArrayList<ArrayList<Float>>> tablelist=new ArrayList<ArrayList<ArrayList<Float>>>();
        tablelist.add(t1);
        tablelist.add(t2);
        tablelist.add(t3);

        ArrayList<String> attname1=new ArrayList<String>(Arrays.asList("id","X","Y"));
        ArrayList<String> attname2=new ArrayList<String>(Arrays.asList("id","R","P"));
        ArrayList<String> attname3=new ArrayList<String>(Arrays.asList("ID","M","Z"));
        ArrayList<ArrayList<String>> attNameList=new ArrayList<ArrayList<String>>();
        attNameList.add(attname1);
        attNameList.add(attname2);
        attNameList.add(attname3);

        ArrayList<ArrayList<String>> condition=new ArrayList<ArrayList<String>>();
        //condition.add(new ArrayList<String>(Arrays.asList("0","X","1","R")));
        //condition.add(new ArrayList<String>(Arrays.asList("0","X","2","M")));
        //condition.add(new ArrayList<String>(Arrays.asList("0","X","2","Z")));
        //condition.add(new ArrayList<String>(Arrays.asList("1","P","2","Z")));

        ArrayList<Float> weight=new ArrayList<Float>(Arrays.asList(new Float(0),new Float(0),new Float(0),new Float(0),new Float(0),new Float(1)));




        TableJoin tj=new TableJoin(k,N,tablelist,attNameList,condition);


        /*tj.resultForTest=  tj.joinTwoTableNew(t1,t2,
                new ArrayList<String>(Arrays.asList("ID","X","Y","Z")),
                new ArrayList<String>(Arrays.asList("ID","R","P")),1,condition);
        System.out.println(tj.resultForTest.table);*/
        System.out.println("**********runJoinTable result ***********");
        ArrayList<ArrayList<Float>> rest=tj.runJoinTable().table;
        for(ArrayList<Float> e:rest){
            System.out.println(e);
        }
        System.out.println("**********TopK from JoinedTable***********");
        tj.runTopKOnJoinedTable(weight);




    }
}
