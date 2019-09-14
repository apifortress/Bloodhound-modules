/**
  * Copyright 2019 API Fortress
  * Licensed under the Apache License, Version 2.0 (the "License");
  * you may not use this file except in compliance with the License.
  * You may obtain a copy of the License at
  *
  * http://www.apache.org/licenses/LICENSE-2.0
  *
  * Unless required by applicable law or agreed to in writing, software
  * distributed under the License is distributed on an "AS IS" BASIS,
  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  * See the License for the specific language governing permissions and
  * limitations under the License.
  *
  * @author Simone Pezzano
  */
package com.apifortress.afthem.config.loaders

import java.util

import com.apifortress.afthem.modules.mongodb.config.loaders.MongoDbConfigLoader
import javax.servlet.http.HttpServletRequest
import org.apache.commons.io.IOUtils
import org.junit.Assert._
import org.junit._
import org.mockito.Mockito._
import org.mongodb.scala.bson.collection.immutable.Document
import org.mongodb.scala.{Completed, MongoClient, MongoClientSettings, Observer, ServerAddress}

object TestMongoDbConfigLoader {
  var client : MongoClient = null

  @BeforeClass
  def initTestDb(): Unit = {
    val settings = MongoClientSettings.builder()
      .applyToClusterSettings(builder =>
        builder.hosts(util.Arrays.asList(new ServerAddress("localhost", 27017)))
      )
      .codecRegistry(MongoClient.DEFAULT_CODEC_REGISTRY)
      .build()
    client = MongoClient(settings)
    val collection = client.getDatabase("afthem").getCollection("test_configuration")
    collection.insertOne(Document(IOUtils.toString(getClass.getResourceAsStream("/flow.json"), "UTF-8"))).subscribe(new Observer[Completed] {
      override def onNext(result: Completed): Unit = {}

      override def onError(e: Throwable) = {}

      override def onComplete(): Unit = {}
    })
    collection.insertOne(Document(IOUtils.toString(getClass.getResourceAsStream("/implementers.json"), "UTF-8"))).subscribe(new Observer[Completed] {
      override def onNext(result: Completed): Unit = {}

      override def onError(e: Throwable) = {}

      override def onComplete(): Unit = {}
    })
    collection.insertOne(Document(IOUtils.toString(getClass.getResourceAsStream("/backend.json"), "UTF-8"))).subscribe(new Observer[Completed] {
      override def onNext(result: Completed): Unit = {}

      override def onError(e: Throwable) = {}

      override def onComplete(): Unit = {}
    })
  }

  @AfterClass
  def cleanup(): Unit = {
    client.getDatabase("afthem").getCollection("test_configuration").drop().subscribe(new Observer[Completed] {
      override def onNext(result: Completed): Unit = {}
      override def onError(e: Throwable) = {}
      override def onComplete(): Unit = {}
    })
    client.close()
  }
}

class TestMongoDbConfigLoader {

  @Test
  def testBackends(): Unit = {
    val request = mock(classOf[HttpServletRequest])
    when(request.getRequestURL).thenReturn(new StringBuffer("http://127.0.0.1/demo/product"))
    val loader = new MongoDbConfigLoader(Map("uri"->"mongodb://localhost", "collection"->"test_configuration"))
    val backends = loader.loadBackends()
    val backend = backends.findByRequest(request)
    assertTrue(backend.isDefined)
    assertEquals("127.0.0.1/demo",backend.get.prefix)
  }

  @Test
  def testFlows(): Unit = {
    val loader = new MongoDbConfigLoader(Map("uri"->"mongodb://localhost","collection"->"test_configuration"))
    val flow = loader.loadFlow("default")
    assertNotNull(flow)
    assertTrue(flow.keySet().contains("proxy/request"))
  }
  @Test
  def testImplementers(): Unit = {
    val loader = new MongoDbConfigLoader(Map("uri"->"mongodb://localhost","collection"->"test_configuration"))
    val implementers = loader.loadImplementers()
    assertNotNull(implementers)
    assertTrue(implementers.implementers.size>1)
  }



}
