package main;
import java.io.*;
import java.util.*;
import java.lang.*;
/**
 * Created by kuozhang on 4/17/16.
 */
public class LoadDataTable {
    /**
     * the data structure that hold the csv data
     */
    private ArrayList<ArrayList<Float>> arrayList= new ArrayList<ArrayList<Float>>();
    /**
     * constructor
     * @param filename
     */
    public LoadDataTable(String filename) throws IOException {
        Scanner input= new Scanner(new File(filename));
//        FileReader input = new FileReader(filename);
//        BufferedReader br = new BufferedReader(input);
//        String line = null;
//        while ((line = br.readLine()) != null) {
            while(input.hasNextLine()){
            Scanner colReader = new Scanner(input.nextLine());
            ArrayList col = new ArrayList();
            while (colReader.hasNextFloat()) {
                col.add(colReader.nextFloat());
                //System.out.print(colReader.nextInt());

            }
            arrayList.add(col);
        }
    }
    public ArrayList<ArrayList<Float>> getTable(){
        return arrayList;
    }
    /**
     *for test
     */
   /* public static void main(String args[]) throws IOException {
        LoadDataTable ldt = null;
        try {
            ldt = new LoadDataTable("/Users/kuozhang/Documents/Buffalo/2016/Course/CSE 562/TestDataSet");
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }
        System.out.print(ldt.arrayList);
    }*/
}
