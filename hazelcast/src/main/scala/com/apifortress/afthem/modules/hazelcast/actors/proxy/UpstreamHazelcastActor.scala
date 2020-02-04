package com.apifortress.afthem.modules.hazelcast.actors.proxy

import java.io.File

import com.apifortress.afthem.actors.AbstractAfthemActor
import com.apifortress.afthem.config.Phase
import com.apifortress.afthem.config.loaders.YamlConfigLoader
import com.apifortress.afthem.messages.beans.HttpWrapper
import com.apifortress.afthem.messages.{WebParsedRequestMessage, WebParsedResponseMessage}
import com.hazelcast.config.{Config, XmlConfigBuilder}
import com.hazelcast.core._

object UpstreamHazelcastActor {

  var instance : HazelcastInstance = _

  def init() : HazelcastInstance = {
    val configPath = YamlConfigLoader.SUBPATH+File.separator+"hazelcast.xml"
    val cfg = if(new File(configPath).exists())
                  new XmlConfigBuilder(configPath).build()
              else
                new Config()
    instance = Hazelcast.newHazelcastInstance(cfg)
    return instance
  }

  def getInstance(): HazelcastInstance = synchronized {
    if(instance == null) {
      return init()
    }
    return instance
  }

}
class UpstreamHazelcastActor(phaseId : String) extends AbstractAfthemActor(phaseId) {


  var reqTopic : ITopic[HttpWrapper] = _
  var resTopic : ITopic[HttpWrapper] = _

  var initialized : Boolean = false

  var tmpReqMsg : WebParsedRequestMessage = _

  UpstreamHazelcastActor.getInstance()

  override def receive: Receive = {
    case msg : WebParsedRequestMessage =>
      loadConfig(getPhase(msg))
      tmpReqMsg = msg
      log.debug("Publishing message to request topic")
      reqTopic.publish(msg.request)
  }

  def loadConfig(phase : Phase): Unit = {
    if(!initialized) {
      val remoteId = phase.getConfigString("remote_id",null)
      log.info(s"Initializing topics for `${remoteId}`")
      reqTopic = UpstreamHazelcastActor.getInstance().getTopic[HttpWrapper]("req-"+remoteId)
      resTopic = UpstreamHazelcastActor.getInstance().getTopic[HttpWrapper]("res-"+remoteId)
      initialized = true
      resTopic.addMessageListener(new MessageListener[HttpWrapper] {
        override def onMessage(message: Message[HttpWrapper]): Unit = {
          getLog.debug("Response is ready. Forwarding to next phase")
          val responseWrapper = message.getMessageObject
          forward(new WebParsedResponseMessage(responseWrapper, tmpReqMsg.request, tmpReqMsg.backend,
            tmpReqMsg.flow, tmpReqMsg.deferredResult, tmpReqMsg.date, tmpReqMsg.meta))
        }
      })
    }
  }
}
