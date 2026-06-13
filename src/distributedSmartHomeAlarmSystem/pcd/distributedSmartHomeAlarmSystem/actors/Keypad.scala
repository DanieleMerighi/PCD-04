package pcd.distributedSmartHomeAlarmSystem.actors

import org.apache.pekko.actor.typed.scaladsl.Behaviors
import org.apache.pekko.actor.typed.{ActorRef, Behavior}


object Keypad:

  type Ref = ActorRef[Command]

  enum Command:
    case Toggle(code: Int)
  
  export Command.*

  def apply(alarmSystem: SmartHomeAlarmSystem.Ref): Behavior[Command] =
    active(using alarmSystem)

  private def active(using alarmSystem: SmartHomeAlarmSystem.Ref): Behavior[Command] =
    Behaviors.receiveMessage:
      case Toggle(code) =>
        alarmSystem ! SmartHomeAlarmSystem.Toggle(code)
        Behaviors.same

