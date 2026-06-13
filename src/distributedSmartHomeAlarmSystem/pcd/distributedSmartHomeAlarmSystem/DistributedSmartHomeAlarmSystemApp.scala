package pcd.distributedSmartHomeAlarmSystem

import org.apache.pekko.actor.typed.ActorSystem
import pcd.distributedSmartHomeAlarmSystem.Sensor.Type.*
import pcd.distributedSmartHomeAlarmSystem.actors.Home


object DistributedSmartHomeAlarmSystemApp:

  @main
  def app(): Unit =
    // First floor
    val kitchenWindow = Sensor("KitchenWindow", Window)
    val loungeWindow = Sensor("LoungeWindow", Window)
    val hallwayWindow = Sensor("HallwayWindow", Window)
    val frontDoor = Sensor("FrontDoor", Door)
    val hallwayMotion = Sensor("HallwayMotion", Motion)
    val loungeMotion = Sensor("LoungeMotion", Motion)
    val kitchenMotion = Sensor("KitchenMotion", Motion)

    val livingArea = Set[Sensor](kitchenWindow,
      loungeWindow,
      hallwayWindow,
      frontDoor,
      hallwayMotion,
      loungeMotion,
      kitchenMotion)

    // Second floor
    val bathroomWindow = Sensor("BathroomWindow", Window)
    val kidsBedroomWindow = Sensor("KidsBedroomWindow", Window)
    val masterBedroomWindow = Sensor("MasterBedroomWindow", Window)
    val bathroomMotion = Sensor("BathroomMotion", Motion)
    val kidsBedroomMotion = Sensor("KidsBedroomMotion", Motion)
    val masterBedroomMotion = Sensor("MasterBedroomMotion", Motion)

    val sleepingArea = Set[Sensor](bathroomWindow,
      kidsBedroomWindow,
      masterBedroomWindow,
      bathroomMotion,
      kitchenMotion,
      masterBedroomMotion)

    val frontYardMotion = Sensor("FrontYardMotion", Motion)
    val backYardMotion = Sensor("BackYardMotion", Motion)
    val shedMotion = Sensor("ShedMotion", Motion)
    val shedDoor = Sensor("ShedDoor", Door)

    val perimeter = Set[Sensor](frontYardMotion,
      backYardMotion,
      shedMotion,
      shedDoor)

    val allSensors = perimeter ++ livingArea ++ sleepingArea
    val home = ActorSystem(Home(allSensors), "Home")

    val pinCode = 1234
    home ! Home.InstallAlarmSystem(pinCode)

    // Example scenario
    // TODO
    home.terminate()

