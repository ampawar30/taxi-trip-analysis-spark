
package gslab.com.taxidata
import java.io.FileWriter

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD
import java.time.YearMonth
import java.time.format.DateTimeFormatter
import java.time.temporal.ChronoUnit._

import gslab.com.topvisited.TopNHotSpot
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.{Dataset, SparkSession}
import utils.TempData
import gslab.com.popularity.PopularPayment
import gslab.com.quarterwise.QuaterWiseData
import gslab.com.canceltrips.TripCancelPerDay
import gslab.com.maxincome.MaxIncomeOptions
import gslab.com.tolls.TollsPaidDateWise
      /** Creates a TaxiDataInfo Contain info of taxi trips and earnings
        *@param taxiID id for taxi
        *@param tripID id of particular trip
        */

      case class TaxiDataInfo(
                         tripID:String,
                         taxiID:String,
                         tripStartTimestamp:String,
                         tripEndTimestamp:String,
                         tripSeconds:Int,  //Int
                         tripMiles:Double,    //Double
                         pickupCensusTract:Int,
                         dropoffCensusTract:Int,
                         pickupCommunityArea:Int,
                         dropoffCommunityArea:Int,//9
                         fare:Double,
                         tips:Double,
                         tolls:Double,
                         extras:Double,
                         tripTotal:Double,//14
                         paymentType:String,
                         company:String,
                         pickupCentroidLatitude:Double,//Double
                         pickupCentroidLongitude:Double,//Double
                         pickupCentroidLocation:String,
                         dropoffCentroidLatitude:Double,//Double
                         dropoffCentroidLongitude:Double,//Double
                         dropoffCentroidLocation:String,
                         communityAreas:Int
                       )
{

}




object TaxiData {


  val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("GSLab_Spark_RDD_Assignment")
  val sc: SparkContext = new SparkContext(conf)
  val fsc= sc.textFile("src/resourcees/Taxi_Trips_10k.csv").filter(!_.contains("Taxi ID"))

  /**
    * mapping from file to case class.
    * taxiRDD contains RDD of Mobile Data.
    *Data checked against null,empty via TempData functions
    * Seq is used if data line is empty then empty Seq of TaxiDataInfo assign otherwise will give error
    */
  val taxiRDD:RDD[Seq[TaxiDataInfo]] = fsc.map {
    line =>
      val nline = line.split(",")
     // println(nline.length)
      if(nline.length>=24) {
        Seq(TaxiDataInfo(nline(0), nline(1), nline(2), nline(3), TempData.toIntOrZero(nline(4)),
          TempData.toDoubleOrZero(nline(5)), TempData.toIntOrZero(nline(6)), TempData.toIntOrZero(nline(7)),
          TempData.toIntOrZero(nline(8)), TempData.toIntOrZero(nline(9)), TempData.fromDollerToDoubleOrZero(nline(10)), TempData.fromDollerToDoubleOrZero(nline(11)),
          TempData.fromDollerToDoubleOrZero(nline(12)), TempData.fromDollerToDoubleOrZero(nline(13)), TempData.fromDollerToDoubleOrZero(nline(14)), nline(15),
          nline(16), TempData.toDoubleOrZero(nline(17)), TempData.toDoubleOrZero(nline(18)),
          nline(19), TempData.toDoubleOrZero(nline(20)), TempData.toDoubleOrZero(nline(21)),
          nline(22), TempData.toIntOrZero(nline(23))))
      }
      else
        {
          Seq.empty
        }
  }.persist()


  def main(args: Array[String]) {
    taxiRDD

    /**
      * taxiRDD contain Seq of TaxiData
      * some data is empty () like this so Seq() of this should not allowd
      * extra data removed by checking size of Seq>1
    */

    val taxiDataList=taxiRDD.filter(_.size>=1)

        // Function call Wait...
//           val grouped=QuaterWiseData.quarterWiseProfit(taxiDataList)
//           val tophotspot=TopNHotSpot.findMostVisitedPlaces(taxiDataList,5)
//           val popularpay=PopularPayment.findMostPopularPaymentMethod(taxiDataList)
//           val popularpaycmp=PopularPayment.findMostPopularPaymentMethodCompanywise(taxiDataList)
//           val canceltrips=TripCancelPerDay.findCancelTripsPerDayCmpWise(taxiDataList)
//           val tollspaiddaywise=TollsPaidDateWise.dayWiseTolls(taxiDataList)
//           val tollPaidMonthWise=TollsPaidDateWise.monthWiseTolls(taxiDataList)
//           val tollPaidYearWise=TollsPaidDateWise.yearWiseTolls(taxiDataList)
//           val tollPaidWeekWise=TollsPaidDateWise.weekWiseTolls(taxiDataList)
           val maxProfitOptions=MaxIncomeOptions.incomeOptionsAreaWise(taxiDataList)
    val grouped=QuaterWiseData.quarterWiseTaxiIncomeReport(taxiDataList)
    sc.stop()
  }


}