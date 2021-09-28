package gslab.com.tolls
import gslab.com.taxidata.TaxiDataInfo
import org.apache.spark.rdd.RDD
import gslab.com.taxidata.TaxiData
import scala.collection.mutable
import org.apache.spark.TaskContext
import java.lang.String
import utils.DateInfo

object TollsPaidDateWise {
  /**
    * Find daywise tolls for company
    * @param taxirdd contains RDD of TaxiTripData.
    * find daywise tolls for company
    *Save Result to File
    */
  def dayWiseTolls(taxirdd: RDD[Seq[TaxiDataInfo]]): Unit = {
    val tollsdata=taxirdd.map(x=>(x(0).company,x(0).tolls,x(0).tripStartTimestamp.split(" ").take(1).toList(0)))//.filter(zerotolls=>zerotolls._2!=0.0 && zerotolls._1!=Nil)
    val tollsdaywise=tollsdata.map(cmpdata=>(cmpdata._1,(cmpdata._3,cmpdata._2)))

    val grpdaytollscmpwise=tollsdaywise.aggregateByKey(List.empty[(Any,Any)])((x,y)=>x:+y,_++_)
    val datewisetolls=grpdaytollscmpwise.map{
      cmpdata=>
        val days=cmpdata._2.map(dtdata=>(dtdata._1.toString,(dtdata._2.toString.toDouble)))
        val para=TaxiData.sc.parallelize(days.toList).reduceByKey((x:Double,y:Double)=>x+y)
        val parareduce=para.map(x=>(x._1,x._2))
        //days.take(5).foreach(println)
        (cmpdata._1,parareduce.take(parareduce.count().toInt).toList)
    }
    datewisetolls.saveAsTextFile("output/daywisetolls")

  }
  /**
    * Find monthwise tolls for company
    * @param taxirdd contains RDD of TaxiTripData.
    * find monthwise tolls for company
    * Save Result to File
    */
  def monthWiseTolls(taxirdd: RDD[Seq[TaxiDataInfo]]): Unit = {
    val tollsdata=taxirdd.map(x=>(x(0).company,x(0).tolls,x(0).tripStartTimestamp.split(" ").take(1).toList(0)))//.filter(zerotolls=>zerotolls._2!=0.0 && zerotolls._1!=Nil)
    //val tollsdaywise=tollsdata.map(cmpdata=>(cmpdata._1,(cmpdata._3,cmpdata._2)))
    val tollsmonthwise=tollsdata.map(cmpdata=>(cmpdata._1,(cmpdata._3.split("/").take(1)(0)+"/"+cmpdata._3.split("/").drop(2)(0),cmpdata._2)))
    tollsmonthwise.take(11).foreach(println)

    val grpmonthtollscmpwise=tollsmonthwise.aggregateByKey(List.empty[(Any,Any)])((x,y)=>x:+y,_++_)
    grpmonthtollscmpwise.take(11).foreach(println)
    val datewisetolls=grpmonthtollscmpwise.map{
      cmpdata=>
        val days=cmpdata._2.map(dtdata=>(dtdata._1.toString,(dtdata._2.toString.toDouble)))
        val para=TaxiData.sc.parallelize(days.toList).reduceByKey((x:Double,y:Double)=>x+y)
        val parareduce=para.map(x=>(x._1,x._2))
        //days.take(5).foreach(println)
        (cmpdata._1,parareduce.take(parareduce.count().toInt).toList)
    }
    datewisetolls.saveAsTextFile("output/monthwisetolls")

  }
  /**
    * Find yearwise tolls for company
    * @param taxirdd contains RDD of TaxiTripData.
    * find yearwise tolls for company
    * Save Result to File
    */
  def yearWiseTolls(taxirdd: RDD[Seq[TaxiDataInfo]]): Unit = {
    val tollsdata=taxirdd.map(x=>(x(0).company,x(0).tolls,x(0).tripStartTimestamp.split(" ").take(1).toList(0)))//.filter(zerotolls=>zerotolls._2!=0.0 && zerotolls._1!=Nil)
    //val tollsdaywise=tollsdata.map(cmpdata=>(cmpdata._1,(cmpdata._3,cmpdata._2)))
    val tollsyearwise=tollsdata.map(cmpdata=>(cmpdata._1,(cmpdata._3.split("/").drop(2)(0),cmpdata._2)))
    val grpmonthtollscmpwise=tollsyearwise.aggregateByKey(List.empty[(Any,Any)])((x,y)=>x:+y,_++_)
    val datewisetolls=grpmonthtollscmpwise.map{
      cmpdata=>
        val days=cmpdata._2.map(dtdata=>(dtdata._1.toString,(dtdata._2.toString.toDouble)))
        val para=TaxiData.sc.parallelize(days.toList).reduceByKey((x:Double,y:Double)=>x+y)
        val parareduce=para.map(x=>(x._1,x._2))
        //days.take(5).foreach(println)
        (cmpdata._1,parareduce.take(parareduce.count().toInt).toList)
    }
    datewisetolls.saveAsTextFile("output/yearwisetolls")

  }
  /**
    * Find weekwise tolls for company
    * @param taxirdd contains RDD of TaxiTripData.
    * find weekwise tolls for company
    * Save Result to File
    */
  def weekWiseTolls(taxirdd: RDD[Seq[TaxiDataInfo]]): Unit = {
    val tollsdata=taxirdd.map(x=>(x(0).company,x(0).tolls,x(0).tripStartTimestamp.split(" ").take(1).toList(0)))//.filter(zerotolls=>zerotolls._2!=0.0 && zerotolls._1!=Nil)
    val tollsdaywise=tollsdata.map(cmpdata=>(cmpdata._1,(cmpdata._3,cmpdata._2)))

    val grpdaytollscmpwise=tollsdaywise.aggregateByKey(List.empty[(Any,Any)])((x,y)=>x:+y,_++_)
    var tasklst:List[TaskContext]=List()
    val datewisetolls=grpdaytollscmpwise.map{
      cmpdata=>
        val days=cmpdata._2.map(dtdata=>(dtdata._1.toString,(dtdata._2.toString.toDouble)))
        //import org.apache.spark.TaskContext
        val tc = TaskContext.get
        tasklst=tasklst:+tc
        val para=TaxiData.sc.parallelize(days.toList).reduceByKey((x:Double,y:Double)=>x+y)
        val parareduce=para.map(x=>(x._1,x._2))
        var mmap:mutable.Map[Int,mutable.Map[Int,Double]]=mutable.Map[Int,mutable.Map[Int,Double]]()

          val  tollsDatesList=parareduce.take(parareduce.count().toInt).toList
          /**
            * one Comapany and their Dates List
            * tollsDateList=List((date,toll),(date,toll))
           */
          for(did<- 0 to tollsDatesList.size-1) {

            var ym=DateInfo.weekAndYear(tollsDatesList(did)._1)
            //println(ym)
            if(mmap.contains(ym._1))
              {
                if(mmap(ym._1).contains(ym._2))
                  {
                    var toll=mmap(ym._1)
                    toll(ym._2)+=tollsDatesList(did)._2
                    mmap=mmap+(ym._1->toll)
                  }
                else
                  {
                    mmap(ym._1)+=(ym._2->tollsDatesList(did)._2)
                  }
              }
            else
            {
              var toll=mutable.Map[Int,Double]()
              //var toll=scala.collection.mutable.Map[Int,Double]=Map(ym._2->tollsDatesList(did)._2.toDouble)
              toll=toll+(ym._2->tollsDatesList(did)._2)
              mmap=mmap+(ym._1->toll)
            }

          }
        (cmpdata._1,mmap)
    }
    datewisetolls
    datewisetolls.take(20).foreach(println)
    //datewisetolls.saveAsTextFile("output/weekwisetolls")
   // while (true){}
  }
}
