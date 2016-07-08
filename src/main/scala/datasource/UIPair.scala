package datasource

/**
  * Created by wushuang on 16-7-8.
  */
class UIPair(uId:String,iId:String) {
  private var _userId = uId
  private var _itemId = iId

  def userId = _userId
  def userId_=(value:String) :Unit = _userId = value

  def itemId = _itemId
  def itemId_=(value:String) :Unit = _itemId = value
}
