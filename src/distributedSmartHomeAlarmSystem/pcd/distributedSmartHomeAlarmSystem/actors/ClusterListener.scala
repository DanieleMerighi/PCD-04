package pcd.distributedSmartHomeAlarmSystem.actors

import org.apache.pekko.actor.typed.Behavior
import org.apache.pekko.actor.typed.scaladsl.Behaviors
import org.apache.pekko.cluster.ClusterEvent.MemberJoined
import org.apache.pekko.cluster.sharding.typed.ShardingEnvelope
import org.apache.pekko.cluster.sharding.typed.scaladsl.{Entity, EntityTypeKey}
import org.apache.pekko.cluster.typed.{Cluster, Subscribe}
import pcd.distributedSmartHomeAlarmSystem.CborSerializable


object ClusterListener:

  val Key = EntityTypeKey[Command]("ClusterListener")
  val Id = "ClusterListener"

  sealed trait Command extends CborSerializable
  case object DoSubscribe extends Command
  private final case class HandleMemberJoined(address: String) extends Command

  def init: Entity[Command, ShardingEnvelope[Command]] =
    Entity(Key)(_ => ClusterListener())

  def apply(): Behavior[Command] =
    Behaviors.setup: context =>
      context.setLoggerName(classOf[ClusterListener.type])
      unsubscribed

  private def unsubscribed: Behavior[Command] =
    Behaviors.receivePartial:
      case (context, DoSubscribe) =>
        val memberEventAdapter = context.messageAdapter[MemberJoined](event => HandleMemberJoined(event.member.address.toString))
        val cluster = Cluster(context.system)
        cluster.subscriptions ! Subscribe(memberEventAdapter, classOf[MemberJoined])
        active

  private def active: Behavior[Command] =
    Behaviors.receivePartial:
      case (context, HandleMemberJoined(address)) =>
        context.log.info(s"Node [$address] joined the cluster.")
        Behaviors.same

