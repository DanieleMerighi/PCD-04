package pcd.distributedSmartHomeAlarmSystem


case class Sensor(name: String, `type`: Sensor.Type)


object Sensor:

  enum Type:
    case Motion
    case Door
    case Window

