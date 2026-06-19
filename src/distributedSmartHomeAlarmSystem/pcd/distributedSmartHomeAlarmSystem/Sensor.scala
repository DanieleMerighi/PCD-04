package pcd.distributedSmartHomeAlarmSystem


case class Sensor(id: String, `type`: Sensor.Type)


object Sensor:

  enum Type:
    case Motion
    case Door
    case Window

