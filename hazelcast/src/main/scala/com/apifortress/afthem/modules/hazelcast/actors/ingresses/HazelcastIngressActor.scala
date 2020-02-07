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
package com.apifortress.afthem.modules.hazelcast.actors.ingresses

import java.io.File
import java.util.Date

import com.apifortress.afthem.AfthemResult
import com.apifortress.afthem.actors.AbstractAfthemActor
import com.apifortress.afthem.config.loaders.YamlConfigLoader
import com.apifortress.afthem.config.{Backends, Flows, Ingress}
import com.apifortress.afthem.exceptions.AfthemSevereException
import com.apifortress.afthem.messages.beans.ExpMap
import com.apifortress.afthem.messages.{WebParsedRequestMessage, WebParsedResponseMessage}
import com.apifortress.afthem.modules.hazelcast.messages.HazelcastTransportMessage
import com.hazelcast.client.HazelcastClient
import com.hazelcast.client.config.{ClientConfig, XmlClientConfigBuilder}
import com.hazelcast.core.{HazelcastInstance, ITopic, Message}

/**
  * The Hazelcast Ingress.
  * A Hazelcast client that connects to a server. The parts are inverted, as the client is actually
  * listening for requests and the server is forwarding those requests.
  * @param phaseId the phase ID
  */
class HazelcastIngressActor(phaseId : String) extends AbstractAfthemActor(phaseId) {

  /**
    * The Hazelcast client
    */
  var client : HazelcastInstance = _

  /**
    * The request topic
    */
  var reqTopic : ITopic[HazelcastTransportMessage] = _
  /**
    * The response topic
    */
  var resTopic : ITopic[HazelcastTransportMessage] = _

  override def receive: Receive = {
    case msg : Ingress =>
      try {
        log.info("Starting Hazelcast Server")
        val name = msg.getConfigString("name", null)

        if (name == null)
          throw new IllegalArgumentException("The name of the client is not in the configuration")

        /**
          * Looking for hazelcast specific configuration in etc/
          * The pattern is [name_of_client]_hazelcastClient.xml. If the file is found, then
          * "address" is required in the config.
          */
        val path = YamlConfigLoader.SUBPATH + File.separator + s"${name}_hazelcastClient.xml"
        val clientConfig = if (new File(path).exists())
          new XmlClientConfigBuilder(path).build()
        else {
          val cc = new ClientConfig
          cc.setInstanceName(name)
          cc.getNetworkConfig.addAddress(msg.getConfigString("server"))
          cc.getNetworkConfig.setConnectionAttemptLimit(0)
          cc
        }

        client = HazelcastClient.newHazelcastClient(clientConfig)
        log.debug(s"Registering Req and Res queues for `${name}")
        reqTopic = client.getTopic[HazelcastTransportMessage]("req-" + name)
        resTopic = client.getTopic[HazelcastTransportMessage]("res-" + name)
        reqTopic.addMessageListener((hazelMessage : Message[HazelcastTransportMessage]) => {
          getLog.debug("Received a new task")
          val afthemResult = new AfthemResult {
            /**
              * Replacing onSetResult so that when "send_back" sends a response, we're
              * publishing that response to the resTopic
              */
            override def onSetResult() = {
              getLog.debug("Got a response from sendback. Sending back")
              resTopic.publish(new HazelcastTransportMessage(this.message.meta.get("__id").get.asInstanceOf[String],this.message.asInstanceOf[WebParsedResponseMessage].response))
            }
          }
          val hazelcastTransportMessage: HazelcastTransportMessage = hazelMessage.getMessageObject
          // find a suitable backend for the inbound URL
          val backendOption = Backends.instance.findByWrapper(hazelcastTransportMessage.wrapper)
          // if one is found then we can proceed
          if (backendOption.isDefined) {
            val backend = backendOption.get
            val flow = Flows.instance.getFlow(backend.flowId)
            val request = new WebParsedRequestMessage(hazelMessage.getMessageObject.wrapper, backend, flow, afthemResult, new Date(), new ExpMap())
            request.meta.put("__id", hazelMessage.getMessageObject.id)
            if (backend.meta != null)
              request.meta ++= backend.meta
            request.meta.put("__start", System.nanoTime())
            log.debug("Sending message to the request actor")
            context.actorSelection("/user/" + msg.next) ! request
          }
        })
      } catch {
        case ex : Exception =>
          throw new AfthemSevereException(null,ex.getMessage)
      }
  }

  override def postStop(): Unit = {
    super.postStop()
    if(client != null)
      client.shutdown()
  }
}
