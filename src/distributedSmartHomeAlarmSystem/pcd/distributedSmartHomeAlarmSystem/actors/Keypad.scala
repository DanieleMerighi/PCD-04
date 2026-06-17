package pcd.distributedSmartHomeAlarmSystem.actors

import org.apache.pekko.actor.typed.Behavior
import org.apache.pekko.actor.typed.scaladsl.Behaviors
import org.apache.pekko.cluster.sharding.typed.ShardingEnvelope
import org.apache.pekko.cluster.sharding.typed.scaladsl.{Entity, EntityRef, EntityTypeKey}
import pcd.distributedSmartHomeAlarmSystem.CborSerializable


object Keypad:

  type Ref = EntityRef[Command]
  val Key = EntityTypeKey[Command]("Keypad")
  val Id = "Keypad"

  sealed trait Command extends CborSerializable
  final case class Toggle(code: Int) extends Command

  def init(alarmSystem: SmartHomeAlarmSystem.Ref): Entity[Command, ShardingEnvelope[Command]] =
    Entity(Key)(_ => Keypad(alarmSystem))
  
  def apply(alarmSystem: SmartHomeAlarmSystem.Ref): Behavior[Command] =
    active(using alarmSystem)

  private def active(using alarmSystem: SmartHomeAlarmSystem.Ref): Behavior[Command] =
    Behaviors.receiveMessage:
      case Toggle(code) =>
        alarmSystem ! SmartHomeAlarmSystem.Toggle(code)
        Behaviors.same

