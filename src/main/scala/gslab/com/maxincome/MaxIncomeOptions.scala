package gslab.com.maxincome

import gslab.com.taxidata.TaxiDataInfo
import org.apache.spark.rdd.RDD

object MaxIncomeOptions {
  /**
    * Calculate Income AreaWise and for miles<=25
    * @param taxirdd contains RDD of TaxiTripData.
    * find areawise list of income and profit.
    * @return modellist return the mobiles data
    */
  def incomeOptionsAreaWise(taxirdd: RDD[Seq[TaxiDataInfo]]): Unit = {
    val areadata = taxirdd.map(x => (x(0).communityAreas, x(0).pickupCommunityArea,x(0).dropoffCommunityArea,x(0).tripMiles,x(0).fare))
    val areaWiseData = areadata.map(cmpdata => (cmpdata._1,((cmpdata._2, cmpdata._3),(cmpdata._4,cmpdata._5))))

    val grpAreaData =areaWiseData.aggregateByKey(List.empty[(Any, Any)])((x, y) => x :+ y, _ ++ _)
    val optionsData=grpAreaData.map{
      area=>
        val milesdata=area._2
        var milessum=0.0.toDouble
        var faresum=0.0.toDouble
        var globalList:List[Any]=List.empty
        var tempList:List[(Double,Double)]=List.empty
        val opt=milesdata.map{
          values=>
            var (miles,fare)=values._2
            milessum=miles.toString.toDouble+milessum
            faresum=fare.toString.toDouble+faresum
            if(milessum>=25)
              {
                globalList=globalList:+(milessum,faresum)
                milessum=0
                faresum=0
                tempList=tempList.drop(tempList.size)
              }
            else
              {
                //tempList=tempList:+(milessum.toString.toDouble,faresum.toString.toDouble)
              }
            if(globalList.nonEmpty) {
              (values._1, globalList(globalList.size-1))
            }
        }
        globalList=globalList.drop(globalList.size)
        (area._1,opt)
    }
    val areawiseprofit=optionsData.map
    {
      x=>
        (x._1,x._2.filter(_.!=()))
    }.filter(_._2.size>0)
   //areawiseprofit.saveAsTextFile("output/maximumIncomeOptions")

  }
}
