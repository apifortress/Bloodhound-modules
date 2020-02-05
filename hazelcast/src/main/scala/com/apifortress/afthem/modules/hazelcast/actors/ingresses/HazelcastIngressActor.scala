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
import com.hazelcast.core.{HazelcastInstance, ITopic, Message, MessageListener}

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
          cc.addAddress(msg.getConfigString("server"))
          cc
        }

        client = HazelcastClient.newHazelcastClient(clientConfig)
        log.debug(s"Registering Req and Res queues for `${name}")
        reqTopic = client.getTopic[HazelcastTransportMessage]("req-" + name)
        resTopic = client.getTopic[HazelcastTransportMessage]("res-" + name)
        reqTopic.addMessageListener(new MessageListener[HazelcastTransportMessage] {
          override def onMessage(hazelMessage: Message[HazelcastTransportMessage]): Unit = {
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
