package tiger.proj01.common

object ActionType extends Enumeration {

  val NONE = Value(-1)
  val SEARCH = Value(0)
  val CLICK = Value(1)
  val ORDER = Value(2)
  val PAY = Value(3)
}