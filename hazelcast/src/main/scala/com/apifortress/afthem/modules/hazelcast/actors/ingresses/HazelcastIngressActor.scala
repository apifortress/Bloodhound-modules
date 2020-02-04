package com.apifortress.afthem.modules.hazelcast.actors.ingresses

import java.io.File
import java.util.Date

import com.apifortress.afthem.AfthemResult
import com.apifortress.afthem.actors.{AbstractAfthemActor, AppContext}
import com.apifortress.afthem.config.loaders.YamlConfigLoader
import com.apifortress.afthem.config.{Backends, Flows, Ingress}
import com.apifortress.afthem.messages.beans.{ExpMap, HttpWrapper}
import com.apifortress.afthem.messages.{WebParsedRequestMessage, WebParsedResponseMessage, WebRawRequestMessage}
import com.hazelcast.client.HazelcastClient
import com.hazelcast.client.config.{ClientConfig, XmlClientConfigBuilder}
import com.hazelcast.core.{HazelcastInstance, ITopic, Message, MessageListener}


class HazelcastIngressActor(phaseId : String) extends AbstractAfthemActor(phaseId) {

  var client : HazelcastInstance = _

  var reqTopic : ITopic[HttpWrapper] = _
  var resTopic : ITopic[HttpWrapper] = _

  override def receive: Receive = {
    case msg : Ingress =>
      log.info("Starting Hazelcast Server")
      val path = YamlConfigLoader.SUBPATH+File.separator+"hazelcastClient.xml"
      val clientConfig = if(new File(path).exists())
                              new XmlClientConfigBuilder(path).build()
                          else {
                            val cc = new ClientConfig
                            cc.setInstanceName(msg.getConfigString("name",null))
                            cc.addAddress(msg.getConfigString("server"))
                            cc
                          }

      client = HazelcastClient.newHazelcastClient(clientConfig)
      reqTopic = client.getTopic[HttpWrapper]("req-"+clientConfig.getInstanceName)
      resTopic = client.getTopic[HttpWrapper]("res-"+clientConfig.getInstanceName)
      reqTopic.addMessageListener(new MessageListener[HttpWrapper] {
        override def onMessage(hazelMessage: Message[HttpWrapper]): Unit = {
          getLog.debug("Received a new task")
          val afthemResult = new AfthemResult {
            override def onSetResult() = {
              getLog.debug("Got a response from sendback. Sending back")
              resTopic.publish(this.message.asInstanceOf[WebParsedResponseMessage].response)
            }
          }
          val requestWrapper = hazelMessage.getMessageObject
          // find a suitable backend for the inbound URL
          val backendOption = Backends.instance.findByWrapper(requestWrapper)
          // if one is found then we can proceed
          if (backendOption.isDefined) {
            val backend = backendOption.get
            val flow = Flows.instance.getFlow(backend.flowId)
            val request = new WebParsedRequestMessage(hazelMessage.getMessageObject,backend,flow,afthemResult,new Date(),new ExpMap())
            if(backend.meta != null)
              request.meta ++= backend.meta
            request.meta.put("__start",System.nanoTime())
            log.debug("Sending message to the request actor")
            context.actorSelection("/user/"+msg.next) ! request
          }
        }
      })
  }

  override def postStop(): Unit = {
    super.postStop()
    client.shutdown()
  }
}
