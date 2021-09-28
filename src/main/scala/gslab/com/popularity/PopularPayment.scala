package gslab.com.popularity
import gslab.com.taxidata.TaxiData
import gslab.com.taxidata.TaxiDataInfo
import org.apache.spark.rdd.RDD

object PopularPayment {
  /**
    * Find Most Popular Payment Method
    * @param taxirdd contains RDD of TaxiTripData.
    * find most Popular Payment Option.
    * print most popular payment method
    */
  def findMostPopularPaymentMethod(taxirdd: RDD[Seq[TaxiDataInfo]]): Unit = {
    val keydata=taxirdd.map(v=>(v(0).paymentType,1))
    val initialCount = 0;
    val addToCounts = (n: Int, v: Int) => n + 1
    val sumPartitionCounts = (p1: Int, p2: Int) => p1 + p2

    val countbykey = keydata.aggregateByKey(initialCount)(addToCounts, sumPartitionCounts)
    val swapcountbykey=countbykey.map(_.swap)
    val sorteddata=swapcountbykey.sortByKey(false,1)
    val outputdata=sorteddata.take(1)
    outputdata.foreach(x=>println("Most Popular Payment Option is",x._2))
  }
  /**
    * find most visited places comapnywise
    * @param taxirdd contains RDD of TaxiTripData.
    * companywise on which area has most popular method of payment
    * save data to files
    */
  def findMostPopularPaymentMethodCompanywise(taxirdd: RDD[Seq[TaxiDataInfo]]): Unit = {
    val cmpdata=taxirdd.map(x=>(x(0).company,(x(0).communityAreas,x(0).paymentType))).groupByKey()
    //val grpcmpdata=cmpdata.map(x=>x._2.map(y=>(x._1,(1,y))))
    val grpcmpdata=cmpdata.map {
      x =>
        val p = x._2.map(y => (1, y))
        val p1 = p.map(_.swap)
        val p2 = TaxiData.sc.parallelize(p1.toList).reduceByKey(_ + _).map(_.swap).sortByKey(false, 1)
        (x._1,p2.map(x => ((x._1, x._2))).take(1).toList)

    }

    grpcmpdata
    grpcmpdata.saveAsTextFile("output/areawisepayforcmp")
    //cmpdata.take(5).foreach(println)
    //grpcmpdata.take(5).foreach(println)
  }
  }
