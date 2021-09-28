package gslab.com.canceltrips

import gslab.com.taxidata.TaxiDataInfo
import org.apache.spark.rdd.RDD
import gslab.com.taxidata.TaxiData
import scala.collection.mutable
import java.lang.String

object TripCancelPerDay
{
  /**
    * Find Out Cancel trips Per Day CompanyWise.
    * @param taxirdd contains RDD of TaxiDataInfo.
    * Calculate companywise cancel trips daywise
    * Save Data to Files
    */
  def findCancelTripsPerDayCmpWise(taxirdd: RDD[Seq[TaxiDataInfo]]): Unit = {
    val cmpdata=taxirdd.filter(x=>x(0).tripMiles<=0.7)
    val cmpareadata= cmpdata.map(cmp=>(cmp(0).company,(cmp(0).communityAreas,cmp(0).tripStartTimestamp.split(" ").take(1).toList)))
    val grpdata=cmpareadata.map(cmp=>(cmp._1,(cmp._2._2(0),cmp._2._1))).groupByKey().filter(x=>x._1!="")
    val grpdata1=cmpareadata.aggregateByKey(List.empty[(Int,List[String])])((x,y)=>x:+y,_++_)
    val grpdata2=cmpareadata.combineByKey((value:(Int,List[String]))=>List(value),
      (aggr: List[(Int,List[String])], value) => aggr ::: (value :: Nil),
      (aggr1: List[(Int,List[String])], aggr2: List[(Int,List[String])] )=> aggr1 ::: aggr2)

    val datecmpwise=grpdata.map
    {
      data=>
        val dates=data._2.toList
        val grpdates=TaxiData.sc.parallelize(dates)
        val grpdata=grpdates.groupByKey
        val areacnt=grpdata.map(x=>(data._1,x._1,x._2.size,x._2.toList))
       // areacnt.foreach(println)
        areacnt.collect().toList
      // grpdates.foreach(println)
    }
    val datecmpwise1=grpdata1.map
    {
      data=>
        val dates=data._2.toList
        val grpdates=TaxiData.sc.parallelize(dates)
        val grpdata=grpdates.groupByKey
        val areacnt=grpdata.map(x=>(data._1,x._1,x._2.size,x._2.toList))
        areacnt.collect().toList
    }
    val datecmpwise2=grpdata2.map
    {
      data=>
        val dates=data._2.toList
        val grpdates=TaxiData.sc.parallelize(dates)
        val grpdata=grpdates.groupByKey
        val areacnt=grpdata.map(x=>(data._1,x._1,x._2.size,x._2.toList))
        areacnt.collect().toList
    }
    /**
     * val list: mutable.MutableList[(String,Int)] = mutable.MutableList.empty
     * val addItemsToList = (s: mutable.MutableList[(String,Int)], v :(java.lang.String,Int)) => s.+=:(v)
     * val mergeLists = (x: mutable.MutableList[(String,Int)],
     * y: mutable.MutableList[(String,Int)]) => x ++= y
    */
      /**
        * reduceByKey
        * val filterdata=cmpareadata.map(cmp=>(cmp._1,Array(cmp._2._2(0),cmp._2._1))).reduceByKey((a,b)=>a+:b)
        * val ftest=filterdata.map(_._2.toString)
        * ftest.take(10).foreach(println)
    */

     //datecmpwise.saveAsTextFile("output/tripcancelcmpwise1")
     //datecmpwise1.saveAsTextFile("output/tripcancelcmpwise2")

  }
}
