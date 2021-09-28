package gslab.com.quarterwise
import gslab.com.taxidata.{TaxiData, TaxiDataInfo}
import org.apache.spark.TaskContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import utils.DateInfo

import scala.collection.mutable

object QuaterWiseData {
  /**
    * Calculate total profit for companies
    * @param taxirdd contains RDD of TaxiTripData.
    * @return audittaxidata return the mobiles data
    */
  def quarterWiseProfit(taxirdd: RDD[Seq[TaxiDataInfo]]): RDD[(String, List[Double])] = {

    val audittaxidata = taxirdd.map(x => (x(0).company, List(x(0).fare, x(0).extras,x(0).tips))).filter(_._1 != "")
    val dates=taxirdd.map(x=>(x(0),x(0).tripStartTimestamp))
    val gdates=dates.groupByKey
    val grpdates=gdates.map
    {
      y=>
        val cmpdates=y._2.map(x=>x)
        cmpdates.flatMap(x=>x.toList)
    }
    val grpauditdata=audittaxidata.groupByKey
    val auditreport=grpauditdata.map { x =>
      val cmpdata=x._2.map(x=>List(x(0),x(1),x(2)))
      //val w=y.map(x=>x(0)+x(1)+x(2))
      val auditfare=cmpdata.map
      {
        farelist=>
          val fare=farelist(0)

          fare
      }
      val auditextra=cmpdata.map
      {
        extralist=>
          val extra=extralist(1)
          extra
      }
      val audittips=cmpdata.map
      {
        tipslist=>
          val tips=tipslist(2)
          tipslist
      }

      //val pp=auditdata
      val fare=auditfare.reduceLeft(_+_)
      val extra=auditextra.reduceLeft(_+_)
      val tips=auditextra.reduceLeft(_+_)
      (x._1,fare,extra,tips,grpdates)
    }

   // auditreport.take(10).foreach(println)


    audittaxidata
    //auditreport
  }
  /**
    * Finds the quarterwise  profit for companies
    * @param taxirdd contains RDD of TaxiTripData.
    * find quarterwise profit for comapny
    * save result to file
    */
  def quarterWiseTaxiIncomeReport(taxirdd: RDD[Seq[TaxiDataInfo]]): Unit = {
    val taxidata = taxirdd.map(x => (x(0).company, 1, x(0).fare, x(0).tips, x(0).tripStartTimestamp.split(" ").take(1).toList(0))) //.filter(zerotolls=>zerotolls._2!=0.0 && zerotolls._1!=Nil)
    val taxiwisedata = taxidata.map(cmpdata => (cmpdata._1, (cmpdata._5,cmpdata._4+cmpdata._3)))
    taxiwisedata.take(10).foreach(println)
    val grpcmpwisedata = taxiwisedata.aggregateByKey(List.empty[(Any, Any)])((x, y) => x :+ y, _ ++ _)
    grpcmpwisedata.take(10).foreach(println)
    val resultdata=grpcmpwisedata.map
    {
      cmpdata=>
        var noofdays=0
        var totalincome=0
        var yearquarter = mutable.Map[String, Double]()
        //Map("year")=Map("Q1",3.3)
        var quarterwise= mutable.Map[Int,mutable.Map[String,Double]]()
        val quartersdata=cmpdata._2.map
        {
          dateincomedata=>

            var takeyear=dateincomedata._1.toString.split("/").reverse(0).toInt
            var quartermonth=dateincomedata._1.toString.split("/")(0).toInt
            var quarter=DateInfo.findQuarter(quartermonth)
            if(quarterwise.contains(takeyear))
              {
                if(quarterwise(takeyear).contains(quarter)) {
                  yearquarter=quarterwise(takeyear)
                  yearquarter(quarter) += dateincomedata._2.toString.toDouble
                  quarterwise(takeyear) = yearquarter
                }
                else
                  {
                    yearquarter(quarter)=dateincomedata._2.toString.toDouble
                    quarterwise(takeyear)=yearquarter
                  }
              }
            else
              {
                yearquarter(quarter)=dateincomedata._2.toString.toDouble
                quarterwise(takeyear)=yearquarter
              }
            yearquarter=yearquarter.empty
            quarterwise
        }
        yearquarter=yearquarter.empty
        quarterwise=quarterwise.empty
        (cmpdata._1,quartersdata.head)
    }
    //resultdata.take(10).foreach(println)
    resultdata.saveAsTextFile("output/quarterwise")
  }
}