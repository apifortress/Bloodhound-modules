/*
 * Copyright 2020 API Fortress
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
package com.apifortress.afthem.modules.hazelcast.actors.proxy

import java.io.File

import com.apifortress.afthem.actors.AbstractAfthemActor
import com.apifortress.afthem.config.loaders.YamlConfigLoader
import com.apifortress.afthem.config.{AfthemCache, Phase}
import com.apifortress.afthem.messages.{WebParsedRequestMessage, WebParsedResponseMessage}
import com.apifortress.afthem.modules.hazelcast.messages.HazelcastTransportMessage
import com.hazelcast.config.{Config, XmlConfigBuilder}
import com.hazelcast.core._
import org.ehcache.Cache

/**
  * Companion object for UpstreamHazelcastActor
  */
object UpstreamHazelcastActor {

  /**
    * An Hazelcast instance
    */
  var instance : HazelcastInstance = _

  val messageCache : Cache[String,WebParsedRequestMessage] = AfthemCache.cacheManager.getCache[String,WebParsedRequestMessage]("hazelcast", classOf[String],classOf[WebParsedRequestMessage])

  /**
    * Initializes the Hazelcast instance
    * @return the Hazelcast instance
    */
  def init() : HazelcastInstance = {
    /**
      * In case etc/ contains hazelcast.xml, then it is used. Otherwise empty configuration is used
      */
    val configPath = YamlConfigLoader.SUBPATH+File.separator+"hazelcast.xml"
    val cfg = if(new File(configPath).exists())
                  new XmlConfigBuilder(configPath).build()
              else
                new Config()
    instance = Hazelcast.newHazelcastInstance(cfg)
    instance
  }

  /**
    * Produces / returns an Hazelcast instance singleton
    * @return a hazelcast instance singleton
    */
  def getInstance(): HazelcastInstance = synchronized {
    if(instance == null) {
      return init()
    }
    return instance
  }

  def getMessageFromCache(id : String) : WebParsedRequestMessage = synchronized[WebParsedRequestMessage] {
    val item = messageCache.get(id)
    if(item != null)
      messageCache.remove(id)
    return item
  }

  /**
    * Shuts down the instance
    */
  def shutdown() : Unit = synchronized {
    if(instance != null){
      instance.shutdown()
      instance = null
    }
  }
}

/**
  * Upstream actor for Hazelcast. The purpose of this actor is to forward a request to an AFtheM alter
  * ego and wait for a response.
  * @param phaseId the phase ID
  */
class UpstreamHazelcastActor(phaseId : String) extends AbstractAfthemActor(phaseId) {

  /**
    * Request topic
    */
  var reqTopic : ITopic[HazelcastTransportMessage] = _

  /**
    * Response topic
    */
  var resTopic : ITopic[HazelcastTransportMessage] = _

  /**
    * True if the Hazelcast system has been intialized
    */
  var initialized : Boolean = false


  /**
    * Initializes the Hazelcast instance
    */
  UpstreamHazelcastActor.getInstance()

  override def receive: Receive = {
    case msg : WebParsedRequestMessage =>
      loadConfig(getPhase(msg))
      log.debug("Publishing message to request topic")
      UpstreamHazelcastActor.messageCache.put(msg.meta("__id").asInstanceOf[String],msg)
      reqTopic.publish(HazelcastTransportMessage(msg.meta("__id").asInstanceOf[String],msg.request))
  }

  /**
    * Load the config and subscribe to queues
    * @param phase the phase
    */
  def loadConfig(phase : Phase): Unit = {
    if(!initialized) {
      val remoteId = phase.getConfigString("remote_id",null)
      log.info(s"Initializing topics for `${remoteId}`")
      reqTopic = UpstreamHazelcastActor.getInstance().getTopic[HazelcastTransportMessage]("req-"+remoteId)
      resTopic = UpstreamHazelcastActor.getInstance().getTopic[HazelcastTransportMessage]("res-"+remoteId)
      initialized = true
      resTopic.addMessageListener((message: Message[HazelcastTransportMessage]) => {
          getLog.debug("Response is ready. Forwarding to next phase")
          val transportMessage = message.getMessageObject
          val tmpMsg = UpstreamHazelcastActor.getMessageFromCache(transportMessage.id)
          if(tmpMsg != null)
            forward(new WebParsedResponseMessage(transportMessage.wrapper, tmpMsg.request, tmpMsg.backend,
              tmpMsg.flow, tmpMsg.deferredResult, tmpMsg.date, tmpMsg.meta))
      })
    }
  }

  override def postStop(): Unit = {
    super.postStop()
    UpstreamHazelcastActor.shutdown()
  }
}