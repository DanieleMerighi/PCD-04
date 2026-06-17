package pcd.distributedSmartHomeAlarmSystem.actors

import org.apache.pekko.actor.typed.Behavior
import org.apache.pekko.actor.typed.scaladsl.Behaviors
import org.apache.pekko.cluster.sharding.typed.ShardingEnvelope
import org.apache.pekko.cluster.sharding.typed.scaladsl.{ClusterSharding, Entity, EntityRef, EntityTypeKey}
import pcd.distributedSmartHomeAlarmSystem.CborSerializable


object SensorActor:

  type Ref = EntityRef[Command]
  val Key = EntityTypeKey[Command]("Sensor")

  sealed trait Command extends CborSerializable
  case object Connect extends Command
  case object Fire extends Command

  def init: Entity[Command, ShardingEnvelope[Command]] =
    Entity(SensorActor.Key)(entityContext => SensorActor(entityContext.entityId))
  
  def apply(id: String): Behavior[Command] =
    Behaviors.setup: context =>
      context.setLoggerName(classOf[SensorActor.type])
      unconnected(using id)

  private def unconnected(using id: String): Behavior[Command] =
    Behaviors.receivePartial:
      case (context, Connect) =>
        val sharding = ClusterSharding(context.system)
        val alarmSystem = sharding.entityRefFor(SmartHomeAlarmSystem.Key, SmartHomeAlarmSystem.Id)
        connected(using id, alarmSystem)

  private def connected(using id: String, alarmSystem: SmartHomeAlarmSystem.Ref): Behavior[Command] =
    Behaviors.receivePartial:
      case (context, Fire) =>
        context.log.info(s"Sensor \"$id\" firing.")
        alarmSystem ! SmartHomeAlarmSystem.HandleSensorFiring(id)
        Behaviors.same

