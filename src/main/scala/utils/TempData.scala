package utils

object TempData
{
  /**
    * Find daywise tolls for company
    * @param s contain String if particular datatype find then return that value for it
    * return either typedData or value associated with that datatype
    */
  def toIntOrZero(s:String):Int=
  {
    try
      {
        s.toInt
      }catch
      {
        case _:NumberFormatException=>0
      }
  }
  def toDoubleOrZero(s:String):Double=
  { try
    {
      s.toDouble
    }catch
    {
      case _:NumberFormatException=>0.0
    }

  }
  def fromDollerToDoubleOrZero(s:String)=
  {
    try
      {
        s.drop(1).toDouble
      }catch
      {
        case _:NumberFormatException=>0.0
      }
  }

  /*  def parse(line: String): TaxiDataInfo = {
      val nline = line.split(",")
      //if(nline.length>=12){println(nline.length)}
      //println(nline.length)

        TaxiDataInfo(nline(0), nline(1), nline(2), nline(3), nline(4).toInt, nline(5).toDouble, nline(6), nline(7), nline(8), nline(9), nline(10), nline(11), nline(12),
          nline(13), nline(14), nline(15), nline(16), nline(17), nline(18), nline(19), nline(20).toDouble, nline(21).toDouble, nline(22),nline(23).toInt)


    }
  */
}
