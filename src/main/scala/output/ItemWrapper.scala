package output

import model.Item
import com.google.common.primitives.Doubles

/**
  * Created by wushuang on 16-7-13.
  */
class ItemWrapper(_item:Item,_value:Double) extends Comparable[ItemWrapper]{
  private var value = _value
  def item = _item
  def getvalue = value
  def setvalue(v:Double):Unit = value = v
  override def compareTo(o:ItemWrapper) = Doubles.compare(value,o.value)
}
