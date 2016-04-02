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

import java.nio.charset.StandardCharsets
import java.nio.file.{FileSystem, Files, Path}

import akka.actor._
import akka.testkit._
import com.fasterxml.jackson.core._
import com.google.common.jimfs.{Configuration, Jimfs}
import org.scalatest._

class JsonParserTest extends TestKit(ActorSystem("JsonParserTest")) with FlatSpecLike with ImplicitSender
  with DefaultTimeout with Matchers with BeforeAndAfterAll {

  def withTestFs(testBody: (FileSystem, Path) => Any) = {
    val testFileSystem = Jimfs.newFileSystem(Configuration.unix())

    val testDir = "/data"
    val testDirPath = testFileSystem.getPath(testDir)
    if (!Files.exists(testDirPath)) Files.createDirectory(testDirPath)

    try {
      testBody(testFileSystem, testDirPath)
    } finally {
      testFileSystem.close()
    }
  }

  def createFile(path: Path, content: String) = Files.write(path, content.getBytes(StandardCharsets.UTF_8))

  "A Parser" should "send BulkPurchases for non-empty json" in withTestFs { (fs, dataDir) =>
    createFile(dataDir.resolve("1.json"), """{
                                            |  "not": "important",
                                            |  "purchases": [
                                            |    {
                                            |      "type": "tour",
                                            |      "amount": 3110
                                            |    },
                                            |    {
                                            |      "type": "hotel",
                                            |      "amount": 758
                                            |    },
                                            |    {
                                            |      "type": "drink",
                                            |      "amount": 5
                                            |    }
                                            |  ]
                                            |}""".stripMargin)

    val probe = TestProbe()
    val act = TestActorRef(new JsonParser(new JsonFactory(), fs,
      dataDir.toAbsolutePath.toString))
    probe.send(act, JsonParser.StartParse(1))
    val m = probe.expectMsgType[JsonParser.BulkPurchases]
    m.pack should (contain ("tour", 3110) and have length 3)
  }

  "A Parser" should "fail on non existent file or path" in withTestFs { (fs, dataDir) =>
    val probe = TestProbe()
    val act = TestActorRef(new JsonParser(new JsonFactory(), fs,
      dataDir.toAbsolutePath.toString))
    probe.send(act, JsonParser.StartParse(3))
    val m = probe.expectMsgType[JsonParser.ReadFailed]
    m.path should include("3.json")
  }

  "A Parser" should "fail if purchases key not exists" in withTestFs { (fs, dataDir) =>
    createFile(dataDir.resolve("1.json"), """{}""")

    val probe = TestProbe()
    val act = TestActorRef(new JsonParser(new JsonFactory(), fs,
      dataDir.toAbsolutePath.toString))
    probe.send(act, JsonParser.StartParse(1))
    val m = probe.expectMsgType[JsonParser.ReadFailed]
    m.path should include("1.json")
  }

  "A Parser" should "fail on invalid JSON" in withTestFs { (fs, dataDir) =>
    createFile(dataDir.resolve("1.json"), """@#$@%!%^%invalidjson""")

    val probe = TestProbe()
    val act = TestActorRef(new JsonParser(new JsonFactory(), fs,
      dataDir.toAbsolutePath.toString))
    probe.send(act, JsonParser.StartParse(1))
    val m = probe.expectMsgType[JsonParser.ReadFailed]
    m.path should include("1.json")
  }

  "A Parser" should "send End if purchases is empty" in withTestFs { (fs, dataDir) =>
    createFile(dataDir.resolve("1.json"), """{"purchases": []}""")

    val probe = TestProbe()
    val act = TestActorRef(new JsonParser(new JsonFactory(), fs,
      dataDir.toAbsolutePath.toString))
    probe.send(act, JsonParser.StartParse(1))
    probe.expectMsg(JsonParser.End(1))
  }

  override def afterAll = {
    super.afterAll
    TestKit.shutdownActorSystem(system)
  }
}
