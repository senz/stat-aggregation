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
import akka.event.LoggingReceive

import scala.util.{Random, Sorting}

object GlobalAggregator {
  sealed trait GlobalAggregator
  case class PurchaseStat(`type`: String, amount: Int, count: Int) extends GlobalAggregator
  case object GetResult extends GlobalAggregator
  case class Result(`type`: String, min: Int, max: Int, avg: Int, median: Int)
}

class GlobalAggregator(`type`: String, sampleSize: Int = 100000) extends Actor with ActorLogging {
  import GlobalAggregator._

  var count = 0
  var min: Int = Int.MaxValue
  var max: Int = Int.MinValue
  var sum: Long = 0
  val samples = new Array[Int](sampleSize)


  override def receive: Receive = LoggingReceive {
    case PurchaseStat(t, a, c) if t == `type` =>
      if (a < min) min = a
      if (a > max) max = a
      sum += (a * c)

      1 to c foreach { _ =>
        val i = if (count < sampleSize) count
        else Random.nextInt(sampleSize)

        samples.update(i, a)
        count += 1
      }

    case GetResult =>
      val (avg: Int, median: Int, min: Int, max: Int) = if (count != 0) {
        ((sum / count).toInt, this.median(), this.min, this.max)
      } // XXX average will be in range of int
      else (0, 0, 0, 0)

      sender() ! Result(`type`, min, max, avg, median)
  }

  def median(): Int = {
    val a: Array[Int] = if (count < samples.length) {
      val a = Array.newBuilder[Int]
      a.sizeHint(count)
      a ++= samples.take(count)
      a.result()
    } else samples
    Sorting.quickSort(a)
    log.debug("samples({}): {}", count, a.mkString(","))

    if (count % 2 == 0) {
      val m = count / 2
      (samples(m - 1) + samples(m)) / 2
    }
    else samples(count / 2)
  }
}
