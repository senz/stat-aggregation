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
import su.senz.stat_aggregator.GlobalAggregator._

class GlobalAggregatorTest extends TestKit(ActorSystem("GlobalAggregatorTest")) with FlatSpecLike with ImplicitSender
  with DefaultTimeout with Matchers with BeforeAndAfterAll {
  "An GlobalAggregator" should "ignore not its types" in {
    val probe = TestProbe()
    val act = TestActorRef(new GlobalAggregator("test", 5))

    val purchases = Seq(
      PurchaseStat("test", 1, 1),
      PurchaseStat("test2", 2, 1)
    )

    purchases foreach { purchaseStat =>
      probe.send(act, purchaseStat)
    }

    probe.send(act, GetResult)
    val res = probe.expectMsgType[Result]
    res.`type` shouldEqual "test"
    res.max shouldEqual purchases.head.amount
  }

  "An GlobalAggregator" should "correctly find median in stream = sample size" in {
    val probe = TestProbe()
    val sampleSize = 5
    val act = TestActorRef(new GlobalAggregator("test", sampleSize))

    val purchases = Seq(
      PurchaseStat("test", 1, 1),
      PurchaseStat("test", 2, 1),
      PurchaseStat("test", 3, 1),
      PurchaseStat("test", 4, 1),
      PurchaseStat("test", 5, 1)
    )

    purchases foreach { purchaseStat =>
      probe.send(act, purchaseStat)
    }

    probe.send(act, GetResult)
    val res = probe.expectMsgType[Result]
    res.median shouldEqual purchases(2).amount
  }

  "An GlobalAggregator" should "correctly find median in stream even and < sample size" in {
    val probe = TestProbe()
    val sampleSize = 5
    val act = TestActorRef(new GlobalAggregator("test", sampleSize))

    val purchases = Seq(
      PurchaseStat("test", 1, 1),
      PurchaseStat("test", 2, 1),
      PurchaseStat("test", 3, 1),
      PurchaseStat("test", 4, 1)
    )

    purchases foreach { purchaseStat =>
      probe.send(act, purchaseStat)
    }

    probe.send(act, GetResult)
    val res = probe.expectMsgType[Result]
    res.median shouldEqual (purchases(1).amount + purchases(2).amount) / 2
  }

  "An GlobalAggregator" should "correctly find min" in {
    val probe = TestProbe()
    val sampleSize = 5
    val act = TestActorRef(new GlobalAggregator("test", sampleSize))

    val purchases = Seq(
      PurchaseStat("test", 1, 1),
      PurchaseStat("test", 2, 1),
      PurchaseStat("test", 3, 1),
      PurchaseStat("test", 4, 1),
      PurchaseStat("test", 5, 1)
    )

    purchases foreach { purchaseStat =>
      probe.send(act, purchaseStat)
    }

    probe.send(act, GetResult)
    val res = probe.expectMsgType[Result]
    res.min shouldEqual purchases.head.amount
  }

  "An GlobalAggregator" should "correctly find max" in {
    val probe = TestProbe()
    val sampleSize = 5
    val act = TestActorRef(new GlobalAggregator("test", sampleSize))

    val purchases = Seq(
      PurchaseStat("test", 1, 1),
      PurchaseStat("test", 2, 1),
      PurchaseStat("test", 3, 1),
      PurchaseStat("test", 4, 1),
      PurchaseStat("test", 5, 1)
    )

    purchases foreach { purchaseStat =>
      probe.send(act, purchaseStat)
    }

    probe.send(act, GetResult)
    val res = probe.expectMsgType[Result]
    res.max shouldEqual purchases.last.amount
  }

  "An GlobalAggregator" should "distribute overloaded elements for median" in {
    val probe = TestProbe()
    val sampleSize = 3

    val act = TestActorRef(new GlobalAggregator("test", sampleSize))

    val purchases = Seq(
      PurchaseStat("test", 1, 1),
      PurchaseStat("test", 2, 1),
      PurchaseStat("test", 3, 1),
      PurchaseStat("test", 4, 1),
      PurchaseStat("test", 5, 1)
    )

    purchases foreach { purchaseStat =>
      probe.send(act, purchaseStat)
    }

    act.underlyingActor.samples.length shouldEqual sampleSize
    act.underlyingActor.samples shouldNot equal (purchases.take(3).map(_.amount).toArray)
  }

  "An GlobalAggregator" should "expand elements with count > 1" in {
    val probe = TestProbe()
    val sampleSize = 5

    val act = TestActorRef(new GlobalAggregator("test", sampleSize))

    val purchases = Seq(
      PurchaseStat("test", 1, 1),
      PurchaseStat("test", 2, 3),
      PurchaseStat("test", 3, 1)
    )

    purchases foreach { purchaseStat =>
      probe.send(act, purchaseStat)
    }

    act.underlyingActor.samples.length shouldEqual sampleSize
    act.underlyingActor.samples shouldEqual Array(1, 2, 2, 2, 3)
  }

  override def afterAll = TestKit.shutdownActorSystem(system)
}
