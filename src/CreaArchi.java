package spaler;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.spark.api.java.function.PairFlatMapFunction;

import scala.Tuple2;

public class CreaArchi implements PairFlatMapFunction<String, Tuple2<String,String>, Tuple2<String,String>> {

    public Iterator<Tuple2<Tuple2<String,String>, Tuple2<String,String>>> call(String sequence) {
        int k = 20;

        List<Tuple2<String, String>> list2 = new ArrayList<>();
        List<Tuple2<Tuple2<String,String>, Tuple2<String,String>>> list = new ArrayList<>();

        for (int i=0; i < sequence.length()-k+1 ; i++) {
            String kmer = sequence.substring(i, k+i);
            String kmerT = kmer.replace('A', 'N').replace('T', 'M').replace('N', 'T').replace('M', 'A').replace('C', 'P').replace('G', 'B').replace('B', 'C').replace('P', 'G');
            if(kmer.compareTo(kmerT)>=0){
                list2.add(new Tuple2<>(kmerT, "H"));
            }else {
                list2.add(new Tuple2<>(kmer, "L"));            }
        }
        for (int i=1; i < sequence.length()-k+1 ; i++) {
            list.add(new Tuple2<>(list2.get(i-1),list2.get(i)));
        }
        return list.iterator();
    }

}
