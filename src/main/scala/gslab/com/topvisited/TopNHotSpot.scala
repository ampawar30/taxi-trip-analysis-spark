package gslab.com.topvisited
import gslab.com.taxidata._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

import scala.collection.mutable.ListBuffer
object TopNHotSpot {

  /**
    * Find most visited places companywise
    * @param taxirdd contains RDD of TaxiTripData.
    * find most visited places
    * Save Result to File
    */
  def findMostVisitedPlaces(taxirdd: RDD[Seq[TaxiDataInfo]],nspot:Int): Unit =
  {
    val communityareadata= taxirdd.map(x => (x(0).company, List(x(0).pickupCommunityArea,x(0).dropoffCommunityArea))).filter(_._1 != "")
    val grpdata=communityareadata.groupByKey()
    val addToSet = (s:String, v: String) => s+v.toString
    val mergePartitionSets = (p1: String, p2:String) => p1 + p2
    //val grp1=communityareadata.reduceByKey(_+_)
    val flatdata=grpdata.map(x=>(x._1,x._2.flatten))
    val flatonedata=flatdata.map{
      x=>
        val cmtarealist=x._2.map(y=>(y.toString,1))
      //val make=makeit(x._2)
        val cmmtdata=TaxiData.sc.parallelize(cmtarealist.toList).reduceByKey(_+_)
        val swpdata=cmmtdata.map(_.swap)
        val hotspotfrq=swpdata.sortByKey(false,1)


        val topNspot=hotspotfrq.map(_._2)
        (x._1,topNspot.take(nspot).toList)
    }



    flatonedata.saveAsTextFile("output/topNhotSpots")

  }

}
