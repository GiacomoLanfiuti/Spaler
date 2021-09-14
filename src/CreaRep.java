package spaler;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.spark.api.java.function.PairFlatMapFunction;

import scala.Tuple2;

public class CreaRep implements PairFlatMapFunction<String, String, Integer> {

	@Override
	public Iterator<Tuple2<String,Integer>> call(String sequence) {
        int k=20;

        List<Tuple2<String,Integer>> list = new ArrayList<Tuple2<String,Integer>>();
        for (int i=0; i < sequence.length()-k+1 ; i++) {
            String kmer = sequence.substring(i, k+i);
            String kmerT = kmer.replace('A', 'N').replace('T', 'M').replace('N', 'T').replace('M', 'A').replace('C', 'P').replace('G', 'B').replace('B', 'C').replace('P', 'G');
            String rep;
            if(kmer.compareTo(kmerT)>=0){
            	rep=kmerT;
            	list.add(new Tuple2<>(rep, 1));
			}else {
				rep=kmer;
            	list.add(new Tuple2<>(rep, 1));
			}
        }         
        return list.iterator();
     }

}
