package model
import java.io.Serializable

import model.PriceType.PriceType

import scala.beans.BeanProperty

/**
  * Created by wushuang on 16-7-8.
  */
class Item extends Serializable{
  private var _prjId : Int = _
  private var _cityId : Int = _
  private var _cityName : String = _
  private var _cityKey : String = _
  private var _listId :Int = _
  private var _name :String = _
  private var _channel:Int = _
  private var _dist : Int = _
  private var _block: Int = _
  private var _mapx: String= _
  private var _mapy: String = _
  private var _price: Int = _
  private var _priceMore: String = _

  /*
  1,地铁
  2,新房
  3,在售
  4,尾盘
  5,售空
  6,租售
  7,在租
  我们关注2,3
  */
  private var _salestat : Int = _
  //非数据库字段
  private var _prob : Double = _
  private var _mapxDouble : Double = _
  private var _mapyDouble : Double = _
  private var _priceType : PriceType = _

  def prjId = _prjId
  def prjId_=(value: Int):Unit = _prjId = value

  def cityId = _cityId
  def cityId_=(value: Int):Unit = _cityId = value

  def cityName = _cityName
  def cityName_=(value: String):Unit = _cityName = value

  def cityKey = _cityKey
  def cityKey_=(value: String):Unit = _cityKey = value

  def listId = _listId
  def listId_=(value: Int):Unit = _listId = value

  def name = _name
  def name_=(value: String):Unit = _name = value

  def channel = _channel
  def channel_=(value: Int):Unit = _channel = value

  def dist = _dist
  def dist_=(value: Int):Unit = _dist = value

  def block = _block
  def block_=(value: Int):Unit = _block = value

  def mapx = _mapx
  def block_=(value :String):Unit = {
    _mapx = value
    try{
      _mapxDouble = _mapx.toDouble
    }catch{
      case ex:Exception =>{
        _mapxDouble = 0
      }
    }
  }

  def mapy = _mapy
  def mapy_=(value :String):Unit = {
    _mapy = value
    try{
      _mapyDouble = _mapy.toDouble
    }catch{
      case ex:Exception =>{
        _mapyDouble = 0
      }
    }
  }

  def price = _price
  def price_=(value: Int):Unit = _price = value

  def priceMore = _priceMore
  def priceMore_=(value : String):Unit = {
    _priceMore = value
    if (value.startsWith("元")){
      _priceType = PriceType.PricePerSQM
    }else if (value.startsWith("万元")){
      _priceType = PriceType.PricePerHouse
    }else{
      _priceType = PriceType.PriceUnknown
    }
  }

  def salestat = _salestat
  def salestat_=(value: Int):Unit = _salestat = value

  def mapxDouble = _mapxDouble
  def mapyDouble = _mapyDouble
  def priceType = _priceType

  def canEqual(a : Any) = a.isInstanceOf[Item]

  override def equals(that : Any):Boolean = that match{
    case that:Item => that.canEqual(this) && this.hashCode == that.hashCode
    case _ => false
  }
  override def hashCode:Int = {
    var result = _prjId
    result = result * 31 + _channel
    return result
  }
}
