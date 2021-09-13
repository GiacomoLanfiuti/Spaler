package spaler

import org.apache.spark.api.java.{JavaPairRDD, JavaRDD}
import org.apache.spark.graphx.{EdgeDirection, EdgeRDD, VertexId, VertexRDD}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.graphframes.GraphFrame
import org.apache.spark.sql._


object scalaPregel {

  def generaDF(k: JavaRDD[Tuple2[java.lang.Long, java.lang.Long]]): GraphFrame = {
    val spark: SparkSession = SparkSession.builder.master("local").getOrCreate
    val edgesDF = spark.createDataFrame(k).toDF("src", "dst")
    val graph = GraphFrame.fromEdges(edgesDF)
    return graph
  }

  def pregelSpaler(g: GraphFrame, v11: DataFrame, vnm: DataFrame): JavaRDD[Row] = {
    val spark: SparkSession = SparkSession.builder.master("local").getOrCreate
    val v11List = v11.javaRDD.collect();
    val vnmList = vnm.javaRDD.collect();

    var graph = g.toGraphX.mapVertices((id, attribute) => (attribute.getLong(0), id.toLong, if (v11List.contains(attribute)) (true) else (false), (-1), if (vnmList.contains(attribute)) (true) else (false)))
    println("Creato graph")
    graph = graph.subgraph(edgeTriplet => (!edgeTriplet.srcAttr._5 || !edgeTriplet.dstAttr._5), (id, attribute) => (!attribute._5))
    System.out.println("Via con il pregel")

    graph = graph.pregel(
      initialMsg = (0.toLong, 0.toLong, false, -1, false),
      maxIterations = Int.MaxValue,
      activeDirection = EdgeDirection.Out
    )(
      //vprog
      (_, attribute, newAttribute) => if (newAttribute._3 & !attribute._3 & attribute._4 == (-1))
        (attribute._1, newAttribute._2, true, newAttribute._4 + 1, newAttribute._5) else if
      (newAttribute._3 & attribute._3 & attribute._4 <= newAttribute._4 & attribute._2 != newAttribute._2)
        (attribute._1, newAttribute._2, true, newAttribute._4 + 1, newAttribute._5) else (attribute),
      //sndMsg
      triplet => {
        if (triplet.srcAttr._3 & triplet.srcAttr._4 >= triplet.dstAttr._4)
          Iterator((triplet.dstId, (triplet.srcAttr._1, triplet.srcAttr._2, triplet.srcAttr._3, triplet.srcAttr._4, triplet.srcAttr._5)))
        else
          Iterator.empty
      },
      //mergeMsg:
      (attribute1, attribute2) => if (attribute1._3 & !attribute2._3) (attribute2._1, attribute1._2, true, attribute1._4 + 1, attribute2._5)
      else if (attribute1._3 & attribute2._3 & attribute2._4 <= attribute1._4 & attribute2._2 != attribute1._2)
        ((attribute2._1, attribute1._2, true, attribute1._4 + 1, attribute2._5)) else (attribute2))
    println()

    System.out.println("Dopo il primo pregel ci sono questi vertici ancora negativi "+graph.vertices.filter(x => (!x._2._3)).count())

    while (graph.vertices.filter(x => (!x._2._3)).count() > 0) {
      val primo = graph.vertices.filter(x => !x._2._3).first()._2._2
      System.out.println("Attiviamo uno dei vertici, per l'esattezza quello con id: "+primo)
      graph = graph.mapVertices((id, attribute) => (attribute._1, attribute._2, if (attribute._3 || attribute._2.equals(primo)) true else false, attribute._4, attribute._5))

      graph = graph.pregel(
        initialMsg = (0.toLong, 0.toLong, false, -1, false),
        maxIterations = Int.MaxValue,
        activeDirection = EdgeDirection.Out
      )(
        //vprog
        (_, attribute, newAttribute) => if (newAttribute._3 & !attribute._3 & attribute._4 == (-1))
          (attribute._1, newAttribute._2, true, newAttribute._4 + 1, newAttribute._5) else if
        (newAttribute._3 & attribute._3 & attribute._4 <= newAttribute._4 & attribute._2 != newAttribute._2)
          (attribute._1, newAttribute._2, true, newAttribute._4 + 1, newAttribute._5) else (attribute),
        //sndMsg
        triplet => {
          if (triplet.srcAttr._3 & triplet.srcAttr._4 >= triplet.dstAttr._4 & triplet.srcAttr._2!=triplet.dstAttr._2)
            Iterator((triplet.dstId, (triplet.srcAttr._1, triplet.srcAttr._2, triplet.srcAttr._3, triplet.srcAttr._4, triplet.srcAttr._5)))
          else
            Iterator.empty
        },
        //mergeMsg:
        (attribute1, attribute2) => if (attribute1._3 & !attribute2._3) (attribute2._1, attribute1._2, true, attribute1._4 + 1, attribute2._5)
        else if (attribute1._3 & attribute2._3 & attribute2._4 <= attribute1._4 & attribute2._2 != attribute1._2)
          ((attribute2._1, attribute1._2, true, attribute1._4 + 1, attribute2._5)) else (attribute2))
      println()
      System.out.println("Ora i vertici attivi sono :"+graph.vertices.filter(x => !x._2._3).count())
    }
    return graph.vertices.values.filter(x => x._3).map(x => Row(x._1, x._2, x._4)).toJavaRDD()
  }

}