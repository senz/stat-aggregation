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

import akka.actor._
import akka.testkit._
import org.scalatest._
import su.senz.stat_aggregator.UserAggregator._

class UserAggregatorTest extends TestKit(ActorSystem("UserAggregatorTest")) with FlatSpecLike with ImplicitSender
  with DefaultTimeout with Matchers with BeforeAndAfterAll {
  "An UserAggregator" should "produce tuples of (purchase type, value)" in {
    val probe = TestProbe()
    val act = TestActorRef(new UserAggregator(1))

    val purchases = Seq(
      ("type1", 1),
      ("type2", 1),
      ("type3", 1),
      ("type4", 1),
      ("type5", 1)
    )

    purchases foreach { t =>
      probe.send(act, Purchase(t._1, t._2))
    }

    probe.send(act, GetResult)
    val res = probe.expectMsgType[UserAggregator.Result]
    res.result should have length purchases.length
  }

  "An UserAggregator" should "aggregate count tuples" in {
    val probe = TestProbe()
    val act = TestActorRef(new UserAggregator(1))

    val purchases = Seq(
      ("type1", 1),
      ("type1", 1),
      ("type1", 1),
      ("type1", 2),
      ("type5", 2)
    )

    purchases foreach { t =>
      probe.send(act, Purchase(t._1, t._2))
    }

    probe.send(act, GetResult)
    val res = probe.expectMsgType[UserAggregator.Result]
    res.result should have length 3
    res.result should contain ("type1", 1, 3)

  }
  override def afterAll = TestKit.shutdownActorSystem(system)
}
