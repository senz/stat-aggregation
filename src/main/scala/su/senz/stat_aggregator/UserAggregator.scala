/*
 * Copyright 2016 Constantine Romanov
 *
 *    Licensed under the Apache License, Version 2.0 (the "License");
 *    you may not use this file except in compliance with the License.
 *    You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 *    Unless required by applicable law or agreed to in writing, software
 *    distributed under the License is distributed on an "AS IS" BASIS,
 *    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *    See the License for the specific language governing permissions and
 *    limitations under the License.
 */

package su.senz.stat_aggregator

import akka.actor.{Actor, ActorLogging}

import scala.collection.mutable

object UserAggregator {
  sealed trait UserAggregator
  case class Purchase(`type`: String, amount: Int) extends UserAggregator
  case object GetResult extends UserAggregator
  case class Result(userId: Int, result: Seq[(String, Int, Int)])
}

class UserAggregator(userId: Int) extends Actor with ActorLogging {
  import UserAggregator._

  val tuples = mutable.HashMap.empty[(String, Int), Int]

  override def receive: Receive = {
    case Purchase(t, a) =>
      val count = tuples.getOrElseUpdate((t, a), 0)
      tuples.update((t, a), count + 1)

    case GetResult =>
      val r = tuples map { case ((t, amount), cnt) =>
        (t, amount, cnt)
      }

      sender() ! Result(userId, r.toSeq)
  }
}
