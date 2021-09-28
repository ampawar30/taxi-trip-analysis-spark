package utils
import java.util.Calendar
import java.text.SimpleDateFormat
import java.util.Date
object DateInfo {
  /**
    * Find year and week from date
    * @param date contains String date.
    * @return year and week for that date
    */
  def weekAndYear(date: String): (Int,Int) = {
    val date1 = new SimpleDateFormat("dd/MM/yyyy").parse(date)
    val cl = Calendar.getInstance
    cl.setTime(date1)
    var week = cl.get(Calendar.WEEK_OF_YEAR)
    var year = cl.get(Calendar.YEAR)
    (year,week)
  }
  def findQuarter(quartermonth:Int):String={
    var quarter=""
    if(quartermonth<=3)
    {
      quarter="Q1"
    }else if(quartermonth>=4&& quartermonth<=6)
    {
      quarter="Q2"
    }else if(quartermonth>=7&&quartermonth<=9)
    {
      quarter="Q3"
    }
    else {
      quarter="Q4"
    }
    quarter
  }
}