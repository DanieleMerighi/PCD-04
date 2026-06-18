package pcd.distributedSmartHomeAlarmSystem.actors

import org.apache.pekko.actor.typed.Behavior
import org.apache.pekko.actor.typed.scaladsl.{ActorContext, Behaviors}
import org.apache.pekko.cluster.sharding.typed.ShardingEnvelope
import org.apache.pekko.cluster.sharding.typed.scaladsl.{Entity, EntityTypeKey}
import pcd.distributedSmartHomeAlarmSystem.CborSerializable


object ShowcaseRunner:

  val Key = EntityTypeKey[Command]("ShowcaseRunner")
  val Id = "ShowcaseRunner"

  sealed trait Command extends CborSerializable
  case object Run extends Command

  def init(showcase: ActorContext[?] => Unit): Entity[Command, ShardingEnvelope[Command]] =
    Entity(Key)(_ => ShowcaseRunner(showcase))

  def apply(showcase: ActorContext[?] => Unit): Behavior[Command] =
    initialized(using showcase)

  private def initialized(using showcase: ActorContext[?] => Unit): Behavior[Command] =
    Behaviors.receiveMessage:
      case Run => running

  private def running(using showcase: ActorContext[?] => Unit): Behavior[Command] =
    Behaviors.setup: context =>
      showcase(context)
      Behaviors.stopped

