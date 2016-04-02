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

import java.nio.channels.FileChannel
import java.nio.file.{FileSystem, Files}

import akka.actor.{Actor, ActorLogging, ActorRef}
import com.fasterxml.jackson.core.{JsonFactory, JsonToken, JsonParser => JacksonParser}
import sun.nio.ch.ChannelInputStream

import scala.collection.mutable
import scala.concurrent.duration._
import scala.language.postfixOps

object JsonParser {
  sealed trait JsonParser
  case class StartParse(userId: Int) extends JsonParser
  case class End(userId: Int) extends JsonParser
  case class ReadFailed(path: String, userId: Int) extends JsonParser
  case class InvalidId(id: Int) extends JsonParser
  case class UserPurchase(userId: Int, `type`: String, amount: Int) extends JsonParser
  case class BulkPurchases(userId: Int, pack: Seq[(String, Int)]) extends JsonParser
}

class JsonParser(jsonFactory: JsonFactory, fs: FileSystem, dataDir: String) extends Actor with ActorLogging {
  import JsonParser._
  val readTimeout = 5 seconds
  val bulkSize = 5

  override def receive: Receive = {
    case StartParse(userId) =>
      val s = sender()
      val pathString = s"$dataDir/$userId.json"
      val path = fs.getPath(pathString)

      if (!Files.exists(path) || !Files.isReadable(path)) s ! ReadFailed(pathString, userId)
      else {
        val channel = FileChannel.open(path)
        val stream = new ChannelInputStream(channel)
        val parser = jsonFactory.createParser(stream)

        try {
          val dl = readTimeout.fromNow
          val arrayFound = findPurchasesArray(parser, dl, userId, s)

          if (arrayFound) {
            var size = 0

            do {
              val bulk = parsePurchases(userId, s, parser, bulkSize)
              size = bulk.size

              if (bulk.nonEmpty) s ! BulkPurchases(userId, bulk)
              else s ! End(userId)
            } while (size > 0)
          } else s ! ReadFailed(pathString, userId)
        } catch {
          case e: Exception => s ! ReadFailed(pathString, userId)
        } finally {
          stream.close()
          channel.close()
        }
      }
  }

  private def findPurchasesArray(parser: JacksonParser, dl: Deadline, userId: Int, s: ActorRef) = {
    var found = false
    while (!parser.isClosed && dl.hasTimeLeft() && parser.getCurrentToken != JsonToken.END_ARRAY && !found) {
      val token = Option(parser.nextToken())

      token match {
        case Some(t) if t == JsonToken.START_ARRAY && parser.getCurrentName == "purchases" =>
          found = true

        case _ => // XXX noop, seek forward
      }
    }
    found
  }

  private def parsePurchases(userId: Int, s: ActorRef, parser: JacksonParser, parseLimit: Int): Seq[(String, Int)] = {
    val arrayBuilder = mutable.ArrayBuilder.make[(String, Int)]()
    arrayBuilder.sizeHint(parseLimit)
    var size = 0

    // seek until array or stream end, or until buffer is filled
    while (!parser.isClosed && parser.getCurrentToken != JsonToken.END_ARRAY && size < parseLimit) {
      parser.nextValue() // XXX move to object start

      if (parser.getCurrentToken == JsonToken.START_OBJECT) parseSinglePurchase(parser) match {
        case Some(t) =>
          arrayBuilder += ((t._1, t._2))
          size += 1

        case _ => // XXX noop, seek forward
      }
    }
    arrayBuilder.result()
  }

  private def parseSinglePurchase(parser: JacksonParser): Option[(String, Int)] = {
    if (parser.getCurrentToken != JsonToken.START_OBJECT) throw new Exception("Not object")

    var `type`: Option[String] = None
    var amount: Option[Int] = None

    var token: Option[JsonToken] = None
    while (!parser.isClosed && parser.getCurrentToken != JsonToken.END_OBJECT) {
      token match {
        case Some(t) if t == JsonToken.FIELD_NAME && parser.getCurrentName == "type" =>
          parser.nextValue()
          `type` = Some(parser.getValueAsString)

        case Some(t) if t == JsonToken.FIELD_NAME && parser.getCurrentName == "amount" =>
          parser.nextValue()
          amount = Some(parser.getValueAsString.toInt) // XXX assuming that input data is a subset of Int

        case _ =>
      }
      token = Option(parser.nextToken)
    }

    for {
      t <- `type`
      a <- amount
    } yield (t, a)
  }
}