package pcd.distributedSmartHomeAlarmSystem.actors

import org.apache.pekko.actor.typed.scaladsl.Behaviors
import org.apache.pekko.actor.typed.Behavior
import org.apache.pekko.cluster.sharding.typed.ShardingEnvelope
import org.apache.pekko.cluster.sharding.typed.scaladsl.{ClusterSharding, Entity, EntityRef, EntityTypeKey}
import pcd.distributedSmartHomeAlarmSystem.{CborSerializable, Sensor}


object Home:

  type Ref = EntityRef[Command]
  val Key = EntityTypeKey[Command]("Home")
  val Id = "Home"

  sealed trait Command extends CborSerializable
  final case class InstallAlarmSystem(pinCode: Int) extends Command
  final case class ToggleAlarmSystem(code: Int) extends Command
  final case class InteractWithSensor(sensor: Sensor) extends Command

  def init(sensors: Set[Sensor]): Entity[Command, ShardingEnvelope[Command]] =
    Entity(Key)(_ => Home(sensors))

  def apply(sensors: Set[Sensor]): Behavior[Command] =
    Behaviors.setup: context =>
      context.setLoggerName(classOf[Home.type])
      unsecured(using sensors)

  private def unsecured(using sensors: Set[Sensor]): Behavior[Command] =
    Behaviors.receivePartial:
      case (context, InstallAlarmSystem(pinCode)) =>
        context.log.info("Installing the Alarm System.")
        val sharding = ClusterSharding(context.system)
        val alarmSystem = sharding.entityRefFor(SmartHomeAlarmSystem.Key, SmartHomeAlarmSystem.Id)
        val keypad = sharding.entityRefFor(Keypad.Key, Keypad.Id)
        sensors.foreach(sensor =>
          val sensorEntity = sharding.entityRefFor(SensorActor.Key, sensor.id)
          sensorEntity ! SensorActor.Connect
        )
        secured(using keypad)

  private def secured(using keypad: Keypad.Ref): Behavior[Command] =
    Behaviors.receivePartial:
      case (context, ToggleAlarmSystem(code)) =>
        val sharding = ClusterSharding(context.system)
        val keypad = sharding.entityRefFor(Keypad.Key, Keypad.Id)
        keypad ! Keypad.Toggle(code)
        Behaviors.same
      case (context, InteractWithSensor(sensor)) =>
        val sharding = ClusterSharding(context.system)
        val sensorEntity = sharding.entityRefFor(SensorActor.Key, sensor.id)
        sensorEntity ! SensorActor.Fire
        Behaviors.same

