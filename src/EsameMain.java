package spaler;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import org.graphframes.GraphFrame;

import org.neo4j.driver.*;

import scala.Tuple2;
import scala.Tuple3;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.Scanner;

public class EsameMain {

    public static void main(String[] args) throws IOException {

        Logger.getLogger("org").setLevel(Level.ERROR);
        Logger.getLogger("akka").setLevel(Level.ERROR);

        SparkSession spark = SparkSession.builder().master("local[*]").appName("FASTdoop Test FASTQ").getOrCreate();
        SparkContext sc = spark.sparkContext();
        JavaSparkContext jsc = new JavaSparkContext(sc);
        spark.sparkContext().setCheckpointDir("check");

        Scanner get_input = new Scanner(System.in);
        System.out.println("Vuoi che venga effettuata la rappresentazione su Neo4j del grafo di De Brujin? (true or false):");
        boolean neo4j = get_input.nextBoolean();

        Configuration inputConf = jsc.hadoopConfiguration();
        inputConf.setInt("look_ahead_buffer_size", 2048);

        //IMPORT DELLE READS
        String inputPath = "data/longjump_2.fastq";

        JavaPairRDD<Text, QRecord> dSequences2 = jsc.newAPIHadoopFile(inputPath,
                FASTQInputFileFormat.class, Text.class, QRecord.class, inputConf);

        JavaPairRDD<String, String> dSequences = dSequences2.values().mapToPair(record -> new Tuple2<>(record.getKey(), record.getValue()));

        System.out.println("Comincia il processo di sequenziamento");

        //PULIZIA DELLE STRINGHE
        JavaRDD<String> dSequenze = dSequences.sample(false, 0.0001).values();
        JavaRDD<String> dSequenzePulite = dSequenze.map(x -> x.replace("N", "A"));
        System.out.println("Le reads ricevute in input sono: "+dSequenzePulite.count());

        //CREAZIONI DEI VERTICI
        int k=10;
        JavaPairRDD<Long,Integer > rep = dSequenzePulite.flatMapToPair(new CreaRep()).mapToPair(x-> new Tuple2<>(conversioneLong(x._1), x._2));
        JavaPairRDD<Long, Integer> vertici = rep.reduceByKey((x,y)-> x+y);
        JavaPairRDD<Long, Integer> verticiRimossi = vertici.filter(x -> x._2==1);

        //CREAZIONI DEGLI ARCHI
        JavaRDD<ArcoPol> archi = dSequenzePulite.flatMapToPair(new CreaArchi()).map(x -> new ArcoPol(conversioneLong(x._1._1()), conversioneLong(x._2._1()), x._1._2(), x._2._2()));
        JavaPairRDD<Long, Long> archiPair = archi.mapToPair(x -> new Tuple2<>(x.getSrc(), x.getDst()));
        JavaRDD<Arco> archiRidotti = archiPair.subtractByKey(verticiRimossi).mapToPair(x-> new Tuple2<>(x._2, x._1)).subtractByKey(verticiRimossi).map(x-> new Arco(x._2, x._1));
        JavaPairRDD<Arco, Integer> conteggioArchi = archiRidotti.mapToPair(x -> new Tuple2<>(x, 1)).reduceByKey((x,y)-> x+y);
        JavaRDD<Tuple2<Long, Long>> archiNoPol = conteggioArchi.map(x -> new Tuple2<>(x._1.getSrc(), x._1.getDst())).filter(x-> !x._1.equals(x._2));

        //CREAZIONE DEL GRAFO DI DE BRUJIN
        System.out.println("CREAZIONE DEL GRAFO DI DE BRUJIN");
        GraphFrame grafoGraph = scalaPregel.generaDF(archiNoPol);
        System.out.println("FATTO");

        //RAPPRESENTAZIONE IN NEO4J DEL GRAFO DI DE BRUJIN APPENA COSTRUITO

        if(neo4j) {
            System.out.println("Creazione liste");
            List<Row> nodiNeo = grafoGraph.vertices().toJavaRDD().collect();
            List<Arco> archiNeo = conteggioArchi.keys().collect();

            //CONNESSIONE CON NEO4J
            System.out.println("TENTATIVO DI CONNESSIONE CON NEO4J");
            String uri = "bolt://localhost:7687";
            AuthToken token = AuthTokens.basic("neo4j", "GEBD2021");
            Driver driver = GraphDatabase.driver(uri, token);
            Session s = driver.session();
            System.out.println("CONNESSIONE CON NEO4J OTTENUTA");

            //CREAZIONE DELLE QUERY PER GENERARE NODI E ARCHI
            System.out.println("Iniziano il running delle query");
            String cqlBase = "match (n) detach delete n";
            s.run(cqlBase);

            for (Row nodo : nodiNeo) {
                String cql = "CREATE (p1:KMer {id_KMer: '" + decodificaLong(nodo.getLong(0),k)+ "'}) return p1";
                s.run(cql);
            }

            for (Arco arco : archiNeo) {
                Long n1 = arco.getSrc();
                Long n2 = arco.getDst();
                String cql = "match (n1:KMer {id_KMer:'" + decodificaLong(n1,k) + "'}), (n2:KMer {id_KMer:'" + decodificaLong(n2,k) + "'}) create (n1)-[e:edge]->(n2) return n1, n2";
                s.run(cql);
            }
            System.out.println("Le query sono state eseguite.");
        }

        //FINE NEO 4J

        //INDIVIDUAZIONE VERTICI V1-1, V1, Vn-m, Vn, Vattivi e Vmorti
        Dataset<Row> v11Data =grafoGraph.inDegrees().filter("inDegree==1").join(grafoGraph.outDegrees().filter("outDegree==1"), "id").select("id");
        Dataset<Row> vnmData= grafoGraph.inDegrees().join(grafoGraph.outDegrees(), "id").filter("inDegree!=1 or outDegree!=1").select("id");
        Dataset<Row> v1Data = grafoGraph.degrees().filter("degree==1").select("id");
        Dataset<Row> vnData = grafoGraph.vertices().except(grafoGraph.inDegrees().select("id")).join(grafoGraph.outDegrees().filter("outDegree>1"), "id").select("id").union(grafoGraph.vertices().except(grafoGraph.outDegrees().select("id")).join(grafoGraph.inDegrees().filter("inDegree>1"), "id").select("id"));

        Dataset<Row> vSpenti = vnmData.union(vnData);
        Dataset<Row> v11Sorround = grafoGraph.edges().join(vSpenti.withColumnRenamed("id", "src"), "src").select("dst").withColumnRenamed("dst", "id").join(v11Data, "id").union(grafoGraph.edges().join(vSpenti.withColumnRenamed("id", "dst"), "dst").select("src").withColumnRenamed("src", "id").join(v11Data, "id")).select("id").distinct();
        Dataset<Row> vAttivi = v11Sorround.union(v1Data);

        System.out.println("Vertici totali: "+grafoGraph.vertices().count());

        System.out.println("Vertici 1-1: "+v11Data.count());
        System.out.println("Vertici 1: "+v1Data.count());
        System.out.println("Vertici n-m: "+vnmData.count());
        System.out.println("Vertici n Alt: "+vnData.count());

        System.out.println("Vertici 1-1 Sorround: "+v11Sorround.count());
        System.out.println("Vertici Attivi: "+vAttivi.count());
        System.out.println("Vertici Disattivi: "+vSpenti.count());

        System.out.println("Inizia la parte su Scala");

        //PREGEL
        JavaRDD<Row> gruppi = scalaPregel.pregelSpaler(grafoGraph, vAttivi, vSpenti);
        System.out.println("Finito il pregel");

        //RICOSTRUZIONE DALL'OUTPUT DEL PREGEL
        JavaRDD<Tuple3<Long, Long, Integer>> gruppi2 = gruppi.map(x-> new Tuple3<>(x.getLong(0),x.getLong(1), x.getInt(2)));
        JavaPairRDD<Integer, Arco> gruppi3 = gruppi2.mapToPair(x-> new Tuple2<>(x._3(), new Arco(x._1(),x._2()))).sortByKey();
        JavaPairRDD<Long, Tuple2<Long, Integer>> gruppi4 = gruppi3.mapToPair(x-> new Tuple2<>(x._2.getDst(), new Tuple2<>(x._2.getSrc(), x._1)));
        JavaPairRDD<Long, Iterable<Tuple2<Long, Integer>>> gruppi5 = gruppi4.groupByKey();

        //COSTRUZIONE DELLE CONTIGS
        JavaPairRDD<Long, String> contigs= gruppi5.mapValues(x -> {
            Iterator<Tuple2<Long, Integer>> setDiRep= x.iterator();
            String contig = decodificaLong(setDiRep.next()._1, k);
            while(setDiRep.hasNext()){
                char successivo= decodificaLong(setDiRep.next()._1, k).charAt(k-1);
                contig = contig + successivo;
            }
            return contig;
        });

        System.out.println(contigs.collect());
        System.out.println("Il numero di contigs generate è: "+contigs.count());
        JavaPairRDD<String, Integer> contigsLength = contigs.mapToPair(x-> new Tuple2<>(x._2, x._2.length()));
        System.out.println("La contig più lunga è formata da: "+contigsLength.values().top(1));
        JavaPairRDD<Long, String> contigsReal = contigs.filter(x -> x._2.length()>k);
        JavaPairRDD<Integer, Integer> sumContig = contigsLength.mapToPair(x -> new Tuple2<Integer, Integer>(1, x._2));
        JavaPairRDD<Integer, Integer> meanContig = sumContig.reduceByKey((x,y)->x+y);
        System.out.println("La lunghezza media delle contigs è: "+(double)(meanContig.values().first()/contigsReal.count()));


    }

    public static long conversioneLong(String kmer) {
        long kmer_enc = 0;

        for (byte b : kmer.getBytes()) {
            kmer_enc <<= 2;
            switch (b) {
                case 'A':
                case 'a':
                    kmer_enc += 0; break;
                case 'C':
                case 'c':
                    kmer_enc += 1; break;
                case 'G':
                case 'g':
                    kmer_enc += 2; break;
                case 'T':
                case 't':
                    kmer_enc += 3; break;
            }
        }
        return kmer_enc;
    }

    public static String decodificaLong(long rep, int k) {
        byte[] repString = new byte[k];

        for (int i = k-1; i >= 0; i--) {
            byte b = (byte) (rep & 0b11);
            rep >>= 2;

            switch (b) {
                case 0b00:
                    repString[i] = 'A'; break;
                case 0b01:
                    repString[i] = 'C'; break;
                case 0b10:
                    repString[i] = 'G'; break;
                case 0b11:
                    repString[i] = 'T'; break;
            }
        }
        return new String(repString);
    }

}