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
package com.apifortress.afthem.modules.fortressforwarders.actors.sidecars.serializers

import com.apifortress.afthem.{AfthemHttpClient, Parsers, ReqResUtil, SpelEvaluator}
import com.apifortress.afthem.actors.sidecars.serializers.AbstractSerializerActor
import com.apifortress.afthem.config.Phase
import com.apifortress.afthem.messages.WebParsedResponseMessage
import org.apache.http.HttpResponse
import org.apache.http.client.methods.HttpPost
import org.apache.http.concurrent.FutureCallback
import org.apache.http.entity.StringEntity
import org.apache.http.util.EntityUtils
import scala.collection.mutable

/**
  * The actor taking care of serializing and sending API responses to the legacy Fortress live testing tool
  * @param phaseId the phase Id
  */
class FortressLiveTestingActor(phaseId : String) extends AbstractSerializerActor(phaseId : String) {

  /**
    * The URL to send the payload to
    */
  var url : String = null

  /**
    * Additional variables to be backed with the payload
    */
  var params : Map[String,String] = null

  override def receive: Receive = {
    case msg: WebParsedResponseMessage =>
      loadConfig(getPhase(msg))
      if(shouldCapture(msg)){
        url = SpelEvaluator.evaluateStringIfNeeded(url,Map("msg"->msg))
        params = SpelEvaluator.evaluateStringIfNeeded(params,Map("msg"->msg))
        val exportableObject = serializeObject(msg)
        performRequest(Parsers.serializeAsJsonString(exportableObject, pretty = false))
      }
  }

  override def loadConfig(phase : Phase) = {
    super.loadConfig(phase)
    url = phase.getConfigString("url",null)
    params = phase.getConfigMap("params").asInstanceOf[Map[String,String]]
  }

  /**
    * Prepares the content in the form of a map
    * @param msg the message
    * @return the map
    */
  def serializeObject(msg : WebParsedResponseMessage): mutable.Map[String,Any] = {
    val map = mutable.Map.empty[String,Any]
    map("params") = if(params != null) params else Map.empty[String,Any]
    map("payload") = ReqResUtil.byteArrayToString(msg.response)
    map("Content-Type") = ReqResUtil.determineMimeFromContentType(ReqResUtil.extractContentType(msg.response,"application/json"))
    return map
  }

  /**
    * Performs the request
    * @param body the stringified JSON of the content to be sent
    */
  def performRequest(body : String) : Unit = {
    val post = new HttpPost(url)
    post.setHeader("content-type", "application/json")
    post.setEntity(new StringEntity(body))
    AfthemHttpClient.execute(post, new FutureCallback[HttpResponse] {
      override def completed(t: HttpResponse): Unit = {
        EntityUtils.consume(t.getEntity)
        getLog.debug("Serialized API conversation forwarded")
      }

      override def failed(e: Exception): Unit = {
        getLog.error("Something went wrong during forwarding", e)
      }

      override def cancelled(): Unit = {}
    })
  }
}
