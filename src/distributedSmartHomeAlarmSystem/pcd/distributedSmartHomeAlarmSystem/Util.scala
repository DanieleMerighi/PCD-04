package pcd.distributedSmartHomeAlarmSystem

import org.apache.pekko.actor.typed.{Behavior, BehaviorInterceptor, TypedActorContext}
import org.apache.pekko.actor.typed.scaladsl.Behaviors

import scala.reflect.ClassTag


extension [A](behavior: Behavior[A])
  def interceptThrow(targetMessage: A, throwable: => Throwable)
                    (implicit interceptMessageClassTag: ClassTag[A]): Behavior[A] =
    Behaviors.intercept(() =>
      new BehaviorInterceptor[A, A]:
        override def aroundReceive(context: TypedActorContext[A],
                                   message: A,
                                   target: BehaviorInterceptor.ReceiveTarget[A]): Behavior[A] =
          if message == targetMessage then
            throw throwable
          else
            target(context, message)
    )(behavior)

