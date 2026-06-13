package pcd.distributedSmartHomeAlarmSystem.actors

import org.apache.pekko.actor.typed.{ActorRef, Behavior}
import org.apache.pekko.actor.typed.scaladsl.{ActorContext, Behaviors}
import pcd.distributedSmartHomeAlarmSystem.{Sensor, associateWith, foreachValue}
import pcd.distributedSmartHomeAlarmSystem.actors.Home.Command.{InstallAlarmSystem, InteractWithSensor}


object Home:

  type Ref = ActorRef[Command]

  enum Command:
    case InstallAlarmSystem(pinCode: Int)
    case ToggleAlarmSystem(code: Int)
    case InteractWithSensor(sensor: Sensor)

  export Command.*

  def apply(sensors: Set[Sensor]): Behavior[Command] =
    Behaviors.setup: context =>
      context.setLoggerName(classOf[Home.type])
      val sensorActors = sensors.associateWith(sensor =>
        context.spawn(SensorActor(sensor), sensor.name)
      )
      unsecured(using sensorActors, context)

  private def unsecured(using sensors: Map[Sensor, SensorActor.Ref],
                        context: ActorContext[Command]): Behavior[Command] =
    Behaviors.receiveMessagePartial:
      case InstallAlarmSystem(pinCode) =>
        context.log.info("Installing the Alarm System.")
        val alarmSystem = context.spawn(SmartHomeAlarmSystem(pinCode), "SmartHomeAlarmSystem")
        val keypad = context.spawn(Keypad(alarmSystem), "Keypad")
        sensors.foreachValue(_ ! SensorActor.Connect(alarmSystem))
        secured(using sensors, keypad)

  private def secured(using sensors: Map[Sensor, SensorActor.Ref],
                      keypad: Keypad.Ref): Behavior[Command] =
    Behaviors.receiveMessagePartial:
      case ToggleAlarmSystem(code) =>
        keypad ! Keypad.Toggle(code)
        Behaviors.same
      case InteractWithSensor(sensor) =>
        sensors(sensor) ! SensorActor.Fire()
        Behaviors.same

