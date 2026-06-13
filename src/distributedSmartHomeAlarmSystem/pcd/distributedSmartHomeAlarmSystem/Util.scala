package pcd.distributedSmartHomeAlarmSystem


extension [A](i: Iterable[A])
  def associateWith[V](valueSelector: A => V): Map[A, V] =
    i.map(key => (key, valueSelector(key))).toMap


extension [K, V](m: Map[K, V])
  def foreachValue(f: V => Unit): Unit =
    m.foreach((_, value) => f(value))

