package pcd.distributedSmartHomeAlarmSystem.actors

import org.apache.pekko.actor.typed.{ActorRef, Behavior}
import org.apache.pekko.actor.typed.scaladsl.Behaviors
import pcd.distributedSmartHomeAlarmSystem.Sensor
import pcd.distributedSmartHomeAlarmSystem.actors.SensorActor.Command.Connect


object SensorActor:

  type Ref = ActorRef[Command]

  enum Command:
    case Connect(alarmSystem: SmartHomeAlarmSystem.Ref)
    case Fire()

  export Command.*

  def apply(sensor: Sensor): Behavior[Command] =
    Behaviors.setup: context =>
      context.setLoggerName(classOf[SensorActor.type])
      unconnected(using sensor)

  private def unconnected(using sensor: Sensor): Behavior[Command] =
    Behaviors.receiveMessagePartial:
      case Connect(alarmSystem) =>
        connected(using sensor, alarmSystem)

  private def connected(using sensor: Sensor, alarmSystem: SmartHomeAlarmSystem.Ref): Behavior[Command] =
    Behaviors.receivePartial:
      case (context, Fire()) =>
        context.log.info(s"Sensor \"${sensor.name}\" firing.")
        alarmSystem ! SmartHomeAlarmSystem.HandleSensorFiring(sensor)
        Behaviors.same

