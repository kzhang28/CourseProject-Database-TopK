package main;
import java.awt.geom.FlatteningPathIterator;
import java.util.*;

/**
 * Created by kuozhang on 4/18/16.
 * This class implement each individual algorithm of Top k query.
 * Need the supported class from Index.java and LoadDataTable.java
 */
public class Algorithm {
    /**
     * data table to hold input data N*M matrix
     */
    private final ArrayList<ArrayList<Float>> listArrayList;
    /**
     * Btree on ID
     */
    private final BTree<Float,ArrayList<Float>> bTree;
    /**
     * Secondary dense B-Tree index on each attri
     * key: Float(attribute value)
     * value:Array of ID
     * @Note since attribute value may contain duplicate, need a list to store all id associated with a particular key
     * @Warning the btree at index 0 corresponds to attribute 1st.
     */
    private final ArrayList<BTree<Float,ArrayList<Float>>> secondaryBtreeList=new ArrayList<BTree<Float,ArrayList<Float>>>();
    /**
     * specify how many tuples to be return
     */
    private final int K;
    /**
     * N is the number of attributes, not include id
     */
    private final int  N;
    /**
     * auxiliary data structure for TA algorithm
     */
/*
    private class AuxiliaryStructure{
        */
/**
         * tuple <attribute value,id>
         *//*

        private class Tuple implements Comparable<Tuple>{
            private final Float value;
            private final Float id;
            public Tuple(Float a,Float b){
                this.value=a;
                this.id=b;
            }
            */
/**
             * @override
             * NOTE: The Collections.sort result will be in anti-nature order like: 9 8 7 6
             *//*

            public int compareTo(Tuple t){
                if (this.value==t.getValue())
                    return 0;
                else if (this.value>t.getValue())
                    return -1;
                else return 1;
            }
            public Float getValue(){
                return this.value;
            }
            public Float getId(){
                return this.id;
            }
        }
        */
/**
         * For each attribute column, sort according to attribute value and make a sorted list of pair <attribute value,ID>(Tuple). Assemble N sorted lists into a holder list
         *//*


        public ArrayList<ArrayList<Tuple>> attributeSortedListIndex=new ArrayList<ArrayList<Tuple>>();//must initialize it using new or null reference exception
        */
/**
         * constructor
         *//*

        public AuxiliaryStructure(){
            int size=listArrayList.size();// how many record in data table
            for(int i=0;i<N;++i){
                ArrayList<Tuple> temp=new ArrayList<Tuple>(size);
                for(int j=0;j<size;++j){
                    temp.add(new Tuple(listArrayList.get(j).get(i+1),listArrayList.get(j).get(0)));
                }
                attributeSortedListIndex.add(temp);
            }
            for(int i=0;i<N;++i){
                //Collections.sort(attributeSortedListIndex.get(i));
                tupleQuickSort(attributeSortedListIndex.get(i),0,size-1);// use my own quick sort function
                //System.exit(1000);
            }
            System.out.println("Information: AuxiliaryStructure has been built successfully!");
        }
    }
*/
    /**
     * Constructor
     */
    public Algorithm(ArrayList<ArrayList<Float>> a,BTree<Float,ArrayList<Float>> b,int k,int n){
        this.listArrayList=a;
        this.bTree=b;
        this.K=k;
        this.N=n;
        buildSecondaryBtree();
    }
    /**
     * build secondary dense Btree on Attribute
     * should be invoked after initialization of a Algorithm instance
     */
    public void buildSecondaryBtree(){
        for(int i=1;i<N+1;++i){
            BTree<Float,ArrayList<Float>> btree_=new BTree<Float,ArrayList<Float>>();// one btree for one attribute
            for(ArrayList<Float> ele: listArrayList){
                Float key=ele.get(i);
                if(btree_.get(key)==null){// key is not in Btree
                    ArrayList<Float> l=new ArrayList<Float>();
                    l.add(ele.get(0));
                    btree_.put(key,l);
                }else{
                    btree_.get(key).add(ele.get(0));//key is in btree than insert a id into it
                }
            }
            secondaryBtreeList.add(btree_);
        }
       //System.out.println("Information: Secondary dense btree on attribute is built");
    }
    /**
     * get secondary btree
     */
    public ArrayList<BTree<Float,ArrayList<Float>>> getSecondaryBtreeList(){
        return secondaryBtreeList;
    }
/*    *//**
     * run threshold Algorithm, do not use Secondary Dense Btree
     * @return an TreeMap <Score, Id list>. Two records may have the same score
     *//*

    public TreeMap<Float,ArrayList<Float>> runThresholdAlgorithm(){
        *//**
         * check if the input data is of legal form. Can be omitted to improve efficiency.
         *//*

        if(!checkLegalTable()||!checkLegalWeight()){
            System.out.println("Check failed!");
            System.exit(1);
        }
        *//**
         * instantiate a auxiliary data structure
         *//*

        AuxiliaryStructure structure=new AuxiliaryStructure();
        HashSet<Float> setPool=new HashSet<Float>();// store the id encountered at each parallel row scan
        HashSet<Float> setPoolFinished=new HashSet<Float>();//store the id that has been computed to get its score
        int numberOfCandidate =0;//indicate the number of candidate records (should be <=K)
        TreeMap<Float,ArrayList<Float>> candidatePool=new TreeMap<Float,ArrayList<Float>>();//K candidate pool: <score,id list>
        float t;// the most recent threshold score
        int size=listArrayList.size(); // how many records in input data
        ArrayList<Float> tempArray =new ArrayList<Float>();
        for(int i=0;i<size;++i){
            ArrayList<Float> l=new ArrayList<Float>(N);// for vector multiply
            for(int j=0;j<N;j++){
                Float idTemp=structure.attributeSortedListIndex.get(j).get(i).getId();
                if(!setPoolFinished.contains(idTemp)){
                    setPool.add(idTemp);// add id to pool for each parallel scan, this id should not been seen before
                }
                l.add(structure.attributeSortedListIndex.get(j).get(i).getValue());
            }
            t=vectorMultiplyThenSum(l,weight,N);
            while(!setPool.isEmpty()){
                float temp;
                Iterator<Float> iterator=setPool.iterator();
                Float id=iterator.next();//get one id from current id set
                setPoolFinished.add(id);
                temp=vectorMultiplyThenSum(bTree.get(id),weight,N,1);//should remove the first entry(ID) in record
                if(numberOfCandidate<K){
                    addToCandidatePool(new Float(temp),id,candidatePool);
                    ++numberOfCandidate;
                }
                else{
                    if(candidatePool.firstKey().floatValue()<temp){
                        candidatePool.pollFirstEntry();// if temp >the least score in candidate pool, then remove the least entry!
                        addToCandidatePool(new Float(temp),id,candidatePool);
                    }
                }
                setPool.remove(id);
            }
            if(numberOfCandidate==K&&candidatePool.firstKey().floatValue()>=t)
                break;
        }
    return candidatePool;
    }*/

    /**
     * run TA algorithm USING dense secondary Btree
     * @return TreeMap <score, [id]>. Using list because two different record may have the same score.
     */
    public TreeMap<Float,ArrayList<Float>> runThresholdAlgUseSecondaryBtree(ArrayList<Float> w){
        ArrayList<Float> weight=w;
        int size=listArrayList.size(); // how many records in input data
        ArrayList<ArrayList<Float>> sortAttrList=new ArrayList<ArrayList<Float>>();// store sorted attribute, size should be N
        for(int i=1;i<N+1;++i){
            ArrayList<Float> arr=new ArrayList<Float>(size);
            for(ArrayList<Float> ele: listArrayList){
                arr.add(ele.get(i));
            }
            sortAttrList.add(arr);
        }
        for(ArrayList<Float> ele:sortAttrList){//sort
            FloatQuickSort(ele,0,size-1);
        }
        HashSet<Float> setPool=new HashSet<Float>();// store the id encountered at each parallel row scan
        HashSet<Float> setPoolFinished=new HashSet<Float>();//store the id that has been computed to get its score
        int numberOfCandidate =0;//indicate the number of candidate records (should be <=K)
        TreeMap<Float,ArrayList<Float>> candidatePool=new TreeMap<Float,ArrayList<Float>>();//K candidate pool: <score,id list>
        float t;// the most recent threshold score
        /**
         * Begin TA Using Secondary Dense B-tree
         */
        for(int i=0;i<size;++i){
            ArrayList<Float> tempArr=new ArrayList<Float>(N);
            for(int j=0;j<N;++j){
                tempArr.add(sortAttrList.get(j).get(i));
                ArrayList<Float> tempListValue=secondaryBtreeList.get(j).get(sortAttrList.get(j).get(i));// get the list from secondary dense Btree on attribute
                for(Float e: tempListValue) {
                    if (!setPoolFinished.contains(e)){//not yet encountered id
                        setPool.add(e);
                    }
                }
            }
            t=vectorMultiplyThenSum(tempArr,weight,N);//threshold
            while(!setPool.isEmpty()){
                float temp;
                Iterator<Float> iterator=setPool.iterator();
                Float id=iterator.next();
                setPoolFinished.add(id);
                temp=vectorMultiplyThenSum(bTree.get(id),weight,N,1);
                if(numberOfCandidate<K){
                    addToCandidatePool(new Float(temp),id,candidatePool);
                    ++numberOfCandidate;
                }else{
                    if(candidatePool.firstKey().floatValue()<temp){
                        if(candidatePool.get(candidatePool.firstKey()).size()>1){// at least two records with the same lowest score, then remove one
                            candidatePool.get(candidatePool.firstKey()).remove(0);// choose one to remove
                        }
                        else{
                            candidatePool.pollFirstEntry();
                        }
                        addToCandidatePool(new Float(temp),id,candidatePool);
                    }
                }
                setPool.remove(id);
            }
            if(numberOfCandidate==K&&candidatePool.firstKey().floatValue()>=t)
                break;
        }
        return candidatePool;
    }
/*    *//**
     * run naive algorithm
     * @return an ordered List that holds the top k result's ID.
     *//*
    public TreeMap<Float,ArrayList<Float>> runNaiveAlgorithm(){
        TreeMap<Float,ArrayList<Float>> treeMap=new TreeMap<Float,ArrayList<Float>>();
        float score;
        Float ID;
        for(int i=listArrayList.size(),j=0;j<i;++j){
            score=vectorMultiplyThenSum(listArrayList.get(j),weight,N,1);
            ID=listArrayList.get(j).get(0);
            addToCandidatePool(new Float(score),ID,treeMap);
        }
        return treeMap;
    }*/
    public TreeMap<Float,ArrayList<Float>> runNaiveAlgorithmNew(ArrayList<Float> w){
        ArrayList<Float> weight=w;
        TreeMap<Float,ArrayList<Float>> treeMap=new TreeMap<Float,ArrayList<Float>>();
        int currentNumberInQuene=0;
        float score;
        Float id;
        for(ArrayList<Float> e: listArrayList){
            score=vectorMultiplyThenSum(e,weight,N,1);
            id=e.get(0);
            if(currentNumberInQuene<K){
                addToCandidatePool(new Float(score),id,treeMap);
                ++currentNumberInQuene;
            }
            else{
                if(treeMap.firstKey().floatValue()<score) {// if score > the least score in queue,
                    if(treeMap.get(treeMap.firstKey()).size()>1){// at least two records with the same lowest score in the queue; Then remove one
                        treeMap.get(treeMap.firstKey()).remove(0);
                        addToCandidatePool(new Float (score),id,treeMap);
                    }else {
                        treeMap.pollFirstEntry();
                        addToCandidatePool(new Float(score),id,treeMap);
                    }

                }
                }
            }
        return treeMap;
        }
    /**
     * check if the number of weight array is of the same size as the number of attributes
     * @return true or false
     */
    private boolean checkLegalWeight(ArrayList<Float> w){
        ArrayList<Float> weight=w;
        if(weight.size()==N&&N==listArrayList.get(1).size()-1) {
            System.out.println("Information: Weight data is legal");
            return true;
        }
        else{
            System.out.println("Error: Weight vector illegal: the dimension of weight vector should be equal to the number of attributes");
            return false;
        }
    }
    /**
     * check if each record has the same number of attribute value, in case of missing data value in a field
     * @return true of false
     */
    private boolean checkLegalTable(){
        for(int i=0,j=listArrayList.size();i<j;i++){
            if(listArrayList.get(i).size()-1!=N){
                System.out.println("Error: a field value is missing! Line number: ");
                System.out.print(i);
                return false;
            }
        }
        System.out.println("Information: Table data is legal");
        return true;
    }
    private float vectorMultiplyThenSum(ArrayList<Float> a, ArrayList<Float> b,int size){
        float sum= (float) 0.0;
        for(int i=0;i<size;++i){
            sum+=a.get(i)*b.get(i);
        }
        return sum;
    }
    /**
     *@overload
     * the whole record contain Id, when computing score the id should be omitted.
     */
    private float vectorMultiplyThenSum(ArrayList<Float> a, ArrayList<Float> b,int size,int offsetFor_a){
        float sum=(float)0.0;
        for(int i=0;i<size;++i)
        {
            sum+=a.get(i+offsetFor_a)* b.get(i);
        }
        return sum;
    }
    /**
     * add a ID to candidate pool
     */

    private void addToCandidatePool(Float score,Float ID,TreeMap<Float,ArrayList<Float>> treeMap){
        if(treeMap.containsKey(score)){
            ArrayList<Float> t=treeMap.get(score);
            t.add(ID);
            treeMap.put(score,t);
        }else{
            ArrayList<Float> t=new ArrayList<Float>();
            t.add(ID);
            treeMap.put(score,t);
        }
    }
    public void printResultTA(TreeMap<Float,ArrayList<Float>> t){
        System.out.println("********************************************* Top K result (Threshold Algorithm)*********************************************");
        System.out.println("[record]                                        [score]  ");
        while(!t.isEmpty()){
            Float score=t.lastKey();
            ArrayList<Float> arr=t.get(score);
            for(int i=0;i<arr.size();++i){
                System.out.print(bTree.get(arr.get(i)));
                System.out.println("            "+score.toString());
            }
            t.remove(score);
        }

    }
    public void printResultForNaiveAlg(TreeMap<Float,ArrayList<Float>> t){
        System.out.println("******************************************** Top K result (Naive) ***************************************************************");
        System.out.println("[record]                                        [score]  ");
        int count=0;
        boolean flag=true;
        while(!t.isEmpty()&&flag){
            Float score=t.lastKey();
            ArrayList<Float> arr=t.get(score);
            for(int i=0;i<arr.size();++i){
                System.out.print(bTree.get(arr.get(i)));
                System.out.println("            "+score.toString());
                count++;
                if(count==K){
                    flag=false;
                    break;
                }
            }
            t.remove(score);
        }

    }
    /**
     *  convert result treeMap to list with score AND print
     */
    public ArrayList<Float> convertTreeMapToArray(TreeMap<Float,ArrayList<Float>> t){
        return null;
    }
/*    public void tupleQuickSort(ArrayList<AuxiliaryStructure.Tuple> tupleArrayList,int p,int q){
        if(p<q){
            int index=tupleQuckSortPartition(tupleArrayList,p,q);
            tupleQuickSort(tupleArrayList,p,index-1);// bug bug bug is here!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
            tupleQuickSort(tupleArrayList,index+1,q);
        }
    }
    public int tupleQuckSortPartition(ArrayList<AuxiliaryStructure.Tuple> arr,int p, int q){
        int size=q-p+1;
        int j=p-1;
        AuxiliaryStructure.Tuple tempTuple;
        Float pivot=arr.get(q).getValue();
        for(int i=p;i<q;++i){
            if(arr.get(i).getValue().compareTo(pivot)>0){
                tempTuple=arr.set(i,arr.get(++j));
                arr.set(j,tempTuple);
            }
        }
        tempTuple=arr.set(q,arr.get(++j));
        arr.set(j,tempTuple);
        //printTupleListValue(arr);

        return j;
    }*/
    public void FloatQuickSort(ArrayList<Float> arr,int p,int q){
        if(p<q){
            int index=FloatQuickSortPartition(arr,p,q);
            FloatQuickSort(arr,p,index-1);
            FloatQuickSort(arr,index+1,q);
        }
    }
    public int FloatQuickSortPartition(ArrayList<Float> arr,int p,int q){
        int size=q-p+1;
        int j=p-1;
        Float pivot=arr.get(q);
        Float swapTemp;
        for(int i=p;i<q;++i){
            if(arr.get(i)>pivot){
                swapTemp=arr.set(i,arr.get(++j));
                arr.set(j,swapTemp);
            }
        }
        swapTemp=arr.set(q,arr.get(++j));
        arr.set(j,swapTemp);
        return j;
    }
/*    *//**
     * for debug
     *//*
    public void printTupleListValue(ArrayList<AuxiliaryStructure.Tuple> t){
        for(int i=0;i<t.size();++i){
            System.out.print(t.get(i).getValue());
            System.out.print("  ");
        }
        System.out.println();
    }*/
}

