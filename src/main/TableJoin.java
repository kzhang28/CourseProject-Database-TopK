package main;

import java.lang.reflect.Array;
import java.lang.reflect.Field;
import java.util.*;

/**
 * Created by kuozhang on 4/24/16.
 * This class is responsible for all operations regarding to table join
 */
public class TableJoin {
    /**
     * top K
     */
    private int K;
    /**
     * number of total attribute
     */
    private int N;
    /**
     * Something like this (dataTable_1,dataTable_2,...,),which is used for holding all the tables for join operation
     * @Note each table contains id column!
     */
    private ArrayList<ArrayList<ArrayList<Float>>> tableList;
    /**
     * Something like this ((id X,Y,Z),(id W,S),...),which stores the attributes's names for each dataTable in tableList
     * @Warning the number of attributes' name in each entry of attrName should be equal to the number of attributes in corresponding entry of tableList
     */
    private ArrayList<ArrayList<String>> attrName;
    /**
     * join condition
     * Make the condition table like this
     *  [
     *  [0,X,2,Y]       --->means:Table0.X=Table2.Y
     *  [1,X,2,Y]       --->means: Table1.X=Table2.Y
     *  [1,Y,2,Z]       --->means:Table1.Y=Table2.Z
     *  [1,N,3,F]
     *  [2,S,3,Q]
     *  [3,F,4,Z]
     *   .
     *   .
     *   .
     *  ]
     */
    private ArrayList<ArrayList<String>> conditionList;
    /**
     * List of Btree on ID
     */
    private ArrayList<BTree<Float,ArrayList<Float>>> btreeOnIDList=new ArrayList<BTree<Float,ArrayList<Float>>>();
    /**
     * secondary dense btree on each attribute for every table
     */
    private ArrayList<ArrayList<BTree<Float,ArrayList<Float>>>> secondaryBtreeOnAttriList= new ArrayList<ArrayList<BTree<Float,ArrayList<Float>>>>();
    /**
     * holding intermediate result with attribute name. see it as a structure like in c++
     */
    public class intermediateTableWithAttr{
        public ArrayList<ArrayList<Float>> table;
        public ArrayList<String> attribute;
        public intermediateTableWithAttr(ArrayList<ArrayList<Float>> a,ArrayList<String> b){
            this.table=a;
            this.attribute=b;
        }
    }
    /**
     * constructor
     */
    public TableJoin(int k,int n,ArrayList<ArrayList<ArrayList<Float>>> a,ArrayList<ArrayList<String>> b,ArrayList<ArrayList<String>> c){
        this.K=k;
        this.N=n;
        this.tableList=a;
        this.attrName=b;
        this.conditionList=c;
        constructSecondaryIndex();
        constructBreeOnID();
    }
    /**
     * construct Btree on id
     */
    public void constructBreeOnID(){
        for(ArrayList<ArrayList<Float>> e:tableList){
            BTree<Float,ArrayList<Float>> bt=new BTree<Float,ArrayList<Float>>();
            for(ArrayList<Float> e1:e){
                bt.put(e1.get(0),e1);
            }
            btreeOnIDList.add(bt);
        }
    }
    /**
     * construct secondary B-tree on attribute
     */
    public void constructSecondaryIndex(){
        Float key;
        for(int i=0;i<tableList.size();++i){// how many table need to be created secondary index on
            ArrayList<ArrayList<Float>> tempTable=tableList.get(i);// the i-th table
            ArrayList<BTree<Float,ArrayList<Float>>> aTemp=new ArrayList<BTree<Float,ArrayList<Float>>>();
            for(int j=1;j<tempTable.get(0).size();++j){//how many attribute for each table, j from 1 since column 0 is ID
                BTree<Float,ArrayList<Float>> bTree = new BTree<Float,ArrayList<Float>>();
                for(int m=0;m<tempTable.size();m++){ //how many record in this table
                    key=tempTable.get(m).get(j);
                    if(bTree.get(key)==null){// key is not in tree
                        ArrayList<Float> l=new ArrayList<>();
                        l.add(tempTable.get(m).get(0));//add id into value list
                        bTree.put(key,l);
                    }
                    else{
                        bTree.get(key).add(tempTable.get(m).get(0));
                    }
                }
                aTemp.add(bTree);
            }
            secondaryBtreeOnAttriList.add(aTemp);
        }
       // System.out.println("Information: secondary B-tree built successfully");
    }
    /*******************************************
     * main method for joining multiple tables!
     * *****************************************/
    public intermediateTableWithAttr runJoinTable(){
        int tableNum=tableList.size();// how many table to join
        ArrayList<ArrayList<String >> conditionLeft=new ArrayList<ArrayList<String >>(conditionList);
        intermediateTableWithAttr interTableWithAttr=new intermediateTableWithAttr(null,null);// intermediate result
        ArrayList<ArrayList<Float>> interTable=tableList.get(0);//intermediate table
        ArrayList<String> interAttr=attrName.get(0);

        for(int i=1;i<tableNum;++i){
            interTableWithAttr=joinTwoTableNew(interTable,tableList.get(i),interAttr,attrName.get(i),i,chooseRelatedCondition(conditionLeft,i));
            interTable=interTableWithAttr.table;
            interAttr=interTableWithAttr.attribute;
        }
        return interTableWithAttr;
    }
    /**
     * run tableJoin and then TopK
     * @param weight whose size should be equal to ...the size of final result table's attribute number
     * @return TreeMap as runNaiveTopK()
     */
    public TreeMap<Float,ArrayList<ArrayList<Float>>> runTopKOnJoinedTable(ArrayList<Float> w){
        intermediateTableWithAttr res=runJoinTable();
        int attriNoWithadditionalID=res.table.get(0).size()-1;
        Index index=new Index();
        index.createBtreeID(res.table);//create btree on id
        /**
         * assemble a new wight list with new size res.table.size()-1.
         * see the first ID as id and all other value as attributes,even though these attributes may conatian redundant "ID"
         * the weight corresponding to redundant "ID" should be set 0
         */
        ArrayList<Float> newWight=new ArrayList<Float>(attriNoWithadditionalID);
        for(int i=0,j=0;i<attriNoWithadditionalID;++i){//begin assemble
            if(!res.attribute.get(i+1).toUpperCase().equals("ID")){
                newWight.add(w.get(j++));
            }
            else{
                newWight.add(new Float(0));
            }
        }
        TreeMap<Float,ArrayList<ArrayList<Float>>> result=naiveAlforJoinedTable(newWight,res.table);
        printTopKOnJoinedTable(result);
        return result;
    }
    private void printTopKOnJoinedTable(TreeMap<Float,ArrayList<ArrayList<Float>>> r){
        while(!r.isEmpty()){
            Float score=r.lastKey();
            ArrayList<ArrayList<Float>> tupleList=r.get(score);
            for(ArrayList<Float> e:tupleList){
                System.out.print(score);
                System.out.print("         ");
                System.out.println(e);
            }
            r.remove(score);
        }
    }
    private float vectorMultiplyThenSum(ArrayList<Float> a, ArrayList<Float> b,int size,int offsetFor_a){
        float sum=(float)0.0;
        for(int i=0;i<size;++i)
        {
            sum+=a.get(i+offsetFor_a)* b.get(i);
        }
        return sum;
    }
    private void addToCandidatePool(Float score,ArrayList<Float> tuple,TreeMap<Float,ArrayList<ArrayList<Float>>> pool){
        if(pool.containsKey(score)){
            ArrayList<ArrayList<Float>> x=pool.get(score);
            x.add(tuple);
            pool.put(score,x);
        }else{
            ArrayList<ArrayList<Float>> x=new ArrayList<ArrayList<Float>>();
            x.add(tuple);
            pool.put(score,x);
        }
    }
    private TreeMap<Float,ArrayList<ArrayList<Float>>> naiveAlforJoinedTable(ArrayList<Float> w,ArrayList<ArrayList<Float>> table){
        TreeMap<Float,ArrayList<ArrayList<Float>>> res=new TreeMap<Float,ArrayList<ArrayList<Float>>>();
        int currentNo=0;
        float score=0;
        for(ArrayList<Float> e:table){
            score=vectorMultiplyThenSum(e,w,w.size(),1);
            if(currentNo<K){
                addToCandidatePool(new Float(score),e,res);
                ++currentNo;
            }
            else{
                if(res.firstKey().floatValue()<score){
                    if(res.get(res.firstKey()).size()>1){
                        res.get(res.firstKey()).remove(res.get(res.firstKey()).size()-1);
                        addToCandidatePool(new Float(score),e,res);
                    }
                    else{
                        res.pollFirstEntry();
                        addToCandidatePool(new Float(score),e,res);
                    }
                }
            }
        }
        return res;
    }
    /**
     * choose the conditions that related to current join
     * @param table_i: be about to join the ith table,
     *        currentConditions: the conditions that left to choose from
     * @return conditions that relate to current round join
     * @Note: table id from 0
     */
    public ArrayList<ArrayList<String>> chooseRelatedCondition(ArrayList<ArrayList<String>> currentConditions, int table_i){
        ArrayList<ArrayList<String>> res=new ArrayList<ArrayList<String>>();
        for(ArrayList<String> e: currentConditions){
            if(e.get(2).equals(Integer.toString(table_i))&&Integer.parseInt(e.get(0))<table_i){// presumption: conditionlist should be: [1 x 2 y] [3 q 4 f] ok; [2 y 1 x] NOT OK
                res.add(e);
            }
        }
        for(ArrayList<String> e1:res){//delete the condition that will be used
            currentConditions.remove(e1);
        }
        return res;
    }
    /**
     * enable two table join
     * @return a table of natural join without removal of columns
     * @param table1,table2,table1 attr,table2 attri, condition
     */
/*    public intermediateTableWithAttr joinTwoTable(ArrayList<ArrayList<Float>> t1,ArrayList<ArrayList<Float>> t2,
                                                     ArrayList<String> t1Att,ArrayList<String> t2Att,
                                                     ArrayList<ArrayList<String>> condition){

        ArrayList<ArrayList<Float>> resultTable=new ArrayList<ArrayList<Float>>();// result table
        ArrayList<String> resultAttr=new ArrayList<String>();//result attribute name, something like [X,Y,Z,R,P,Q]
        ArrayList<ArrayList<Integer>> refinedCondition=analysisCondition(condition,t1Att,t2Att);// get mapped condition
        boolean flag=true;
        for(int i=0,s=t1.size();i<s;++i){
            for(int j=0,ss=t2.size();j<ss;++j){
                for(int m=0;m<refinedCondition.size();++m){// begin check each condition
                    if(!t1.get(i).get(refinedCondition.get(m).get(0)).equals(t2.get(j).get(refinedCondition.get(m).get(1)))){
                        flag=false;
                        break;
                    }
                }
                if(flag){// satisfy all condition, merge into a new record
                    resultTable.add(mergeRecord(t1.get(i),t2.get(j)));
                    //System.out.println("One record added into result");
                }
                else
                    flag=true;
            }
        }
        for(String s:t1Att){
            resultAttr.add(s);
        }
        for(String s:t2Att){
            resultAttr.add(s);
        }
        return new intermediateTableWithAttr(resultTable,resultAttr);
    }*/
    /**
     * Join two table using secondary dense Btree, indexJoin!
     * @Param table1,table2,
     *        attr1,attr2 (including ID)
     *        index2: // the index of btreeList corresponding to t2 in secondaryBtreeOnAttriList
     *        condition
     */
    public intermediateTableWithAttr joinTwoTableNew(ArrayList<ArrayList<Float>> t1,ArrayList<ArrayList<Float>> t2,
                                                     ArrayList<String> t1Att,ArrayList<String> t2Att,//contain id
                                                     int index2,// the index of t2 in secondaryBtreeOnAttriList, the index2_th table.
                                                     ArrayList<ArrayList<String>> condition){

        ArrayList<ArrayList<Float>> resultTable=new ArrayList<ArrayList<Float>>();// result table
        ArrayList<String> resultAttr=new ArrayList<String>();//result attribute name, something like[id,X,Y,Z,id,R,P,Q]
        ArrayList<ArrayList<Integer>> refinedCondition=analysisConditionNew(condition,t1Att,t2Att,index2);
        Integer tempInteger,tempInteger1;
        ArrayList<Float> tempList;
        ArrayList<Float> resultIDList;
        Boolean flag;
        ArrayList<HashSet<Float>> setPoolList=new ArrayList<HashSet<Float>>();// list of all setPool in each condition round

        for(ArrayList<Float> e: t1){
            if(refinedCondition.isEmpty()){//no condition, loop join
               for(ArrayList<Float> ee:t2){
                   resultTable.add(mergeRecord(e,ee));
               }
            }else {// indexJoin
                flag=true;// if need to assemble tuples
                for (ArrayList<Integer> e1 : refinedCondition) {
                    tempInteger = e1.get(0);// attribute index in t1
                    tempInteger1 = e1.get(1);
                    //                                 find BtreeList  find Btree
                    tempList = secondaryBtreeOnAttriList.get(index2).get(tempInteger1 - 1).get(e.get(tempInteger));
                    if (tempList == null) { // no tuple (that satisfy this round' condition) in table2 found
                        flag = false;
                        break;// no need to do further check, since all condition round should be satisfied in order to produce one joined tuple
                    } else {// has some tuple satisfying condition in this round.
                        HashSet<Float> setPool = new HashSet<Float>(tempList);// the set of id of tuples in table2 that satisfy this round's condition
                        setPoolList.add(setPool);
                    }
                }
                if (flag) {
                    resultIDList = setIntersection(setPoolList);
                    if (!resultIDList.isEmpty()) {
                        for (Float e4 : resultIDList) {
                            resultTable.add(mergeRecord(e, btreeOnIDList.get(index2).get(e4)));
                        }
                    }
                }
                setPoolList.clear();

            }
        }
        for(String s:t1Att){
            resultAttr.add(s);
        }
        for(String s:t2Att){
            resultAttr.add(s);
        }
        return new intermediateTableWithAttr(resultTable,resultAttr);
    }
    /**
     * set intersection
     */
    public ArrayList<Float> setIntersection(ArrayList<HashSet<Float>> s){
        for(int i=1; i<s.size();++i){
            s.get(0).retainAll(s.get(i));
            if(s.get(0).isEmpty()){
                break;
            }
        }
        return new ArrayList<Float>(s.get(0));
    }
    /**
     * convert condition list to a list with only two column
     * Meaning: ex, [1 2] means the 1th attribute in table 1 ==2th attribute in table 2
     * @warning this method is used only by joinTwoTable
     * @param condition list, attr name list, attri name list (including ID)
     * @return something like this:
     *          [[1,2] [2,3],...]
     */
/*
    private static ArrayList<ArrayList<Integer>> analysisCondition(ArrayList<ArrayList<String>> c, ArrayList<String> t1Att, ArrayList<String> t2Att){
        ArrayList<ArrayList<Integer>> map= new ArrayList<ArrayList<Integer>>();
        String _a,_b;
        for(int i=0;i<c.size();++i){
            _a=c.get(i).get(1);// get X ---> attribute name
            _b=c.get(i).get(3);// get R ---> attribute name
            map.add(new ArrayList<Integer>(Arrays.asList(t1Att.indexOf(_a),t2Att.indexOf(_b))));//add [1,2] to map
        }
        System.out.println(map);
        return map;

    }
*/
    /**
     *@param table_i means now analyze conditions with respect to: intermediateTable (containing attributes from table 0 to table i-1) JOIN table i
     *       example: table_i=3 means that parameter t1Att contains attributes from [table 0 join table 1 join table2]
     */
    private ArrayList<ArrayList<Integer>> analysisConditionNew(ArrayList<ArrayList<String>> c,ArrayList<String> t1Att,ArrayList<String> t2Att,int table_i) {
        ArrayList<ArrayList<Integer>> map = new ArrayList<ArrayList<Integer>>();
        int x,y;
        int firstTableindex;
        for (ArrayList<String> e : c) {
            firstTableindex = Integer.parseInt(e.get(0));// in this round, the attribute is in which table (in case table 1 table 2 contain the same attribute name)
            x=getIndexFromJoinedAttribute(firstTableindex,e.get(1),t1Att);
            y=t2Att.indexOf(e.get(3));
            if(x==-1){
                System.out.println("Error: get index error: index=-1");
                System.exit(1);
            }
            map.add(new ArrayList<Integer>(Arrays.asList(x,y)));
        }
        //System.out.println(map);
        return map;
    }
    /**
     * Only used by analysisConditionNew
     * OriginalTableIndex=i means the result should be got from the i+1 id to i+2 id [...id X Y Z id....]
     */
    private int getIndexFromJoinedAttribute(int originalTableIndex,String s,ArrayList<String> joinedAttr){
        int count=originalTableIndex+1;//the id should be encountered before get the index
        for(int i=0;i<joinedAttr.size();++i){
            if(joinedAttr.get(i).toUpperCase().equals("ID")){
                --count;
            }
            else{
                if(count==0){// can begin find index
                    if(joinedAttr.get(i).equals(s))
                        return i;
                }
            }
        }
        return -1;// not find the attribute name return -1 to indicate error
    }
    /**
     *  Merge two record:
     *  contain ID
     */
    private static ArrayList<Float> mergeRecord(ArrayList<Float> a,ArrayList<Float> b){
        ArrayList<Float> res=new ArrayList<Float>(a.size()+b.size());
        for(Float e:a)
            res.add(e);
        for(Float e:b)
            res.add(e);
        return res;
    }
/*    *//**
     * cut ID (first column) from original Table
     * @return a new table without ID colunm
     *//*
    public static ArrayList<ArrayList<Float>> cutID(ArrayList<ArrayList<Float>> a){
        ArrayList<ArrayList<Float>> res=new ArrayList<ArrayList<Float>>(a.size());
        int len=a.get(0).size();
        for(int i=0;i<a.size();++i){
            res.add(new ArrayList<Float>(a.get(i).subList(1,len)));
        }
        return res;
    }*/
/*
    public static void main(String [] args){
        Float [] f={new Float(1),new Float(2),new Float(3)};
        Float [] k={new Float(4),new Float(5)};
        ArrayList<Float> a=new ArrayList<Float>(Arrays.asList(f));
        ArrayList<Float> b=new ArrayList<Float>(Arrays.asList(k));
        System.out.println(mergeRecord(a,b));

    }
*/
}
