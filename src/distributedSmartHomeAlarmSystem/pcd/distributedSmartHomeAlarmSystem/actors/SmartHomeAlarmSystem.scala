package pcd.distributedSmartHomeAlarmSystem.actors

import org.apache.pekko.actor.typed.{ActorRef, Behavior}
import org.apache.pekko.actor.typed.scaladsl.{ActorContext, Behaviors, TimerScheduler}
import pcd.distributedSmartHomeAlarmSystem.Sensor

import scala.concurrent.duration.{DurationInt, FiniteDuration}


object SmartHomeAlarmSystem:

  private val exitDelayDuration: FiniteDuration = 20.second
  private val entryDelayDuration: FiniteDuration = 10.second

  type Ref = ActorRef[Command]

  enum Command:
    case Toggle(code: Int)
    private[SmartHomeAlarmSystem] case HandleDelayEnd()
    case HandleSensorFiring(source: Sensor)

  import Command.*
  export Command.{HandleDelayEnd as _, *}

  def apply(pinCode: Int): Behavior[Command] =
    Behaviors.setup: context =>
      context.setLoggerName(classOf[SmartHomeAlarmSystem.type])
      Behaviors.withTimers: timers =>
        disarmed(using pinCode, timers)

  private def disarmed(using pinCode: Int, timers: TimerScheduler[Command]): Behavior[Command] =
    Behaviors.receivePartial:
      case (context, Toggle(code)) =>
        context.log.info(s"Arming the Alarm System.")
        checkingPinCode(code, () =>
          timers.startSingleTimer((), HandleDelayEnd(), exitDelayDuration)
          exitDelay
        )

  private def exitDelay(using pinCode: Int, timers: TimerScheduler[Command]): Behavior[Command] =
    Behaviors.receivePartial(
      disarmingOnPinCode
        .orElse:
          case (_, HandleDelayEnd()) => armed
    )

  private def armed(using pinCode: Int, timers: TimerScheduler[Command]): Behavior[Command] =
    Behaviors.receivePartial(
      disarmingOnPinCode
        .orElse:
          case (_, HandleSensorFiring(sensor)) =>
            timers.startSingleTimer((), HandleDelayEnd(), entryDelayDuration)
            entryDelay
    )

  private def entryDelay(using pinCode: Int, timers: TimerScheduler[Command]): Behavior[Command] =
    Behaviors.receivePartial(
      disarmingOnPinCode
        .orElse:
          case (context, HandleDelayEnd()) =>
            context.log.info(s"!!! Alarm !!!")
            alarm
    )

  private def alarm(using pinCode: Int, timers: TimerScheduler[Command]): Behavior[Command] =
    Behaviors.receivePartial(
      disarmingOnPinCode
    )

  private def disarmingOnPinCode(using pinCode: Int,
                                 timers: TimerScheduler[Command]): PartialFunction[(ActorContext[Command], Command), Behavior[Command]] =
    case (context, Toggle(code)) =>
      context.log.info(s"Disarming the Alarm System.")
      checkingPinCode(code, () => disarmed)

  private def checkingPinCode(code: Int, action: () => Behavior[Command])(using pinCode: Int): Behavior[Command] =
    if code == pinCode then
      action()
    else
      Behaviors.same

