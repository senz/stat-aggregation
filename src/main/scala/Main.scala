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

import java.nio.file.FileSystems

import akka.actor.{Actor, ActorLogging, ActorRef, ActorSystem, ExtendedActorSystem, Props, ReceiveTimeout}
import akka.event.LoggingReceive
import com.fasterxml.jackson.core.{JsonFactory, JsonParser => JacksonParser}
import com.typesafe.config.{Config, ConfigFactory}
import su.senz.stat_aggregator.{GlobalAggregator, JsonParser, UserAggregator}

import scala.concurrent.duration._
import scala.language.postfixOps
import scala.util.control.NonFatal

object Main extends App {
  if (args.length != 1) {
    println("you need to provide exactly one argument: path to data dir")
  } else {
    val config = ConfigFactory.defaultApplication().getConfig("application")
    val system = ActorSystem("Main")
    try {
      val app = system.actorOf(Props(new GodActor(args(0), config)), "app")
      val terminator = system.actorOf(Props(classOf[akka.Main.Terminator], app), "app-terminator")
    } catch {
      case NonFatal(e) ⇒ system.terminate(); throw e
    }
  }
}

class GodActor(dataDir: String, config: Config) extends Actor with ActorLogging {
  val anonThreshold = config.getInt("anonThreshold")
  val lastUserId = config.getInt("lastUserId")
  val firstUserId = config.getInt("firstUserId")
  var processedUsers = 0
  var totalUsers = (lastUserId - firstUserId) + 1
  val resultType = config.getString("filterType")
  val parser = context.actorOf(Props(new JsonParser(new JsonFactory(), FileSystems.getDefault, dataDir)), "parser")
  val userAggregators = new Array[ActorRef](totalUsers)
  val globalAggregator = context.actorOf(Props(new GlobalAggregator(resultType)), "global_agg")
  log.debug("Total users {}, fid {}, lid {}", totalUsers, firstUserId, lastUserId)

  override def receive = LoggingReceive {
    case JsonParser.BulkPurchases(id, pack) =>
      val agg = Option(userAggregators(aggIdx(id))).getOrElse {
        val agg = context.actorOf(Props(new UserAggregator(id)), s"agg_$id")
        userAggregators.update(aggIdx(id), agg)
        agg
      }

      pack foreach { p =>
        agg ! UserAggregator.Purchase(p._1, p._2)
      }

    case JsonParser.End(id) =>
      Option(userAggregators(aggIdx(id))) foreach { agg =>
        agg ! UserAggregator.GetResult
      }

    case UserAggregator.Result(id, res) =>
      processedUsers += 1
      // anonymizing and filtering
      res filterNot(_._3 <= anonThreshold) filter(_._1 == resultType) foreach { case (typ, amt, cnt) =>
          globalAggregator ! GlobalAggregator.PurchaseStat(typ, amt, cnt)
      }

      val idx = aggIdx(id)
      Option(userAggregators(idx)) foreach { agg =>
        context.stop(agg)
        userAggregators.update(idx, null)
      }

      if (processedUsers == totalUsers) globalAggregator ! GlobalAggregator.GetResult

    case JsonParser.ReadFailed(p, id) =>
      log.error("Failed to parse {}", p)
      val idx = aggIdx(id)
      Option(userAggregators(idx)) foreach { agg =>
        context.stop(agg)
        userAggregators.update(idx, null)
      }
      processedUsers += 1
      if (processedUsers == totalUsers) globalAggregator ! GlobalAggregator.GetResult

    case r: GlobalAggregator.Result =>
      log.info("result: min: {}, max: {}, avg: {}, median: {}", r.min, r.max, r.avg, r.median)
      context.stop(self)

    case ReceiveTimeout =>
      log.error("Receive timeout, dying")
      context.stop(self)
  }

  @scala.throws[Exception](classOf[Exception])
  override def preStart(): Unit = {
    context.setReceiveTimeout(5 second)
    firstUserId to lastUserId foreach { id =>
      val a = context.actorOf(Props(new UserAggregator(id)), s"agg_$id")
      userAggregators.update(aggIdx(id), a)
      parser ! JsonParser.StartParse(id)
    }
  }

  private def aggIdx(id: Int): Int = {
    if (firstUserId != 0) id - firstUserId else id
  }
}
