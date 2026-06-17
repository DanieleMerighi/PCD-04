package pcd.distributedSmartHomeAlarmSystem

import com.typesafe.config.{Config, ConfigFactory, ConfigValueFactory}
import org.apache.pekko.actor.CoordinatedShutdown
import org.apache.pekko.actor.typed.scaladsl.{ActorContext, Behaviors}
import org.apache.pekko.actor.typed.{ActorSystem, Behavior}
import org.apache.pekko.cluster.sharding.typed.scaladsl.ClusterSharding
import org.apache.pekko.cluster.typed.Cluster
import org.apache.pekko.event.slf4j.Logger
import pcd.distributedSmartHomeAlarmSystem.Sensor.Type.*
import pcd.distributedSmartHomeAlarmSystem.actors.*


object DistributedSmartHomeAlarmSystemApp:

  private def startup[A](baseConfig: Config, port: Int, rootBehavior: Behavior[A]) =
    val config = baseConfig.withValue("pekko.remote.artery.canonical.port", ConfigValueFactory.fromAnyRef(port))
    ActorSystem(rootBehavior, "ClusterSystem", config)

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

    val pinCode = 1234

    val rootBehavior = Behaviors.setup: context =>

      val sharding = ClusterSharding(context.system)
      sharding.init(ClusterListener.init)
      val clusterListener = sharding.entityRefFor(ClusterListener.Key, ClusterListener.Id)
      clusterListener ! ClusterListener.DoSubscribe

      Thread.sleep(10000)

      sharding.init(Home.init(allSensors))
      val home = sharding.entityRefFor(Home.Key, Home.Id)
      sharding.init(SensorActor.init)
      sharding.init(SmartHomeAlarmSystem.init(pinCode))
      val alarmSystem = sharding.entityRefFor(SmartHomeAlarmSystem.Key, SmartHomeAlarmSystem.Id)
      sharding.init(Keypad.init(alarmSystem))

      val showcase = (context: ActorContext[?]) => {

        val cluster = Cluster(context.system)
        context.log.info(s"Cluster now has ${cluster.state.members.size} nodes.")
        Thread.sleep(2000)

        // Example scenario

        home ! Home.InstallAlarmSystem(pinCode)
        Thread.sleep(1000)
        home ! Home.ToggleAlarmSystem(pinCode)
        Thread.sleep(5000)

        home ! Home.InteractWithSensor(loungeMotion) // homeowner roaming around
        home ! Home.InteractWithSensor(hallwayMotion)

        Thread.sleep(5000)

        home ! Home.ToggleAlarmSystem(pinCode)
        Thread.sleep(1000)
        home ! Home.InteractWithSensor(frontDoor) // on the way out
        home ! Home.InteractWithSensor(frontYardMotion)

        Thread.sleep(5000)

        home ! Home.InteractWithSensor(frontYardMotion) // on the way in
        home ! Home.InteractWithSensor(frontDoor)
        Thread.sleep(1000)
        home ! Home.ToggleAlarmSystem(pinCode)

        Thread.sleep(5000)

        home ! Home.InteractWithSensor(kitchenMotion) // roaming around

        Thread.sleep(5000)

        home ! Home.ToggleAlarmSystem(pinCode)

        Thread.sleep(20000)

        home ! Home.InteractWithSensor(backYardMotion) // intruder
        Thread.sleep(5000)
        home ! Home.InteractWithSensor(loungeWindow)
        Thread.sleep(2000)
        home ! Home.InteractWithSensor(loungeMotion)
        Thread.sleep(4000)
        home ! Home.InteractWithSensor(hallwayMotion) // gets caught

        Thread.sleep(10000)

        context.log.info("Simulating failure...")
        alarmSystem ! SmartHomeAlarmSystem.ForceFailure

        Thread.sleep(2000)

        home ! Home.ToggleAlarmSystem(pinCode)

        Thread.sleep(1000)

        context.log.info("Beginning graceful shutdown...")
        val _ = CoordinatedShutdown(context.system)
          .run(CoordinatedShutdown.clusterLeavingReason)
      }
      sharding.init(ShowcaseRunner.init(showcase))
      val showcaseRunner = sharding.entityRefFor(ShowcaseRunner.Key, ShowcaseRunner.Id)

      Thread.sleep(2000)

      showcaseRunner ! ShowcaseRunner.Run
      Behaviors.empty

    Logger.root.info("Starting seed nodes...")
    val baseConfig = ConfigFactory.load("application.conf")
    val seeds = Seq(7354, 7355, 7356)
    seeds.foreach(startup(baseConfig, _, rootBehavior))

