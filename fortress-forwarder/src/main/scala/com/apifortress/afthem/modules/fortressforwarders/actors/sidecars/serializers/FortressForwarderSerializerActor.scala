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

import com.apifortress.afthem.actors.sidecars.serializers.AbstractSerializerActor
import com.apifortress.afthem.config.Phase
import com.apifortress.afthem.messages.WebParsedResponseMessage
import com.apifortress.afthem.{AfthemHttpClient, AfthemResponseSerializer, Parsers}
import org.apache.http.HttpResponse
import org.apache.http.client.methods.HttpPost
import org.apache.http.concurrent.FutureCallback
import org.apache.http.entity.StringEntity
import org.apache.http.util.EntityUtils

import scala.collection.mutable.ListBuffer

/**
  * Actor that serializes an API conversation and sends over to an HTTP endpoint
  * @param phaseId the phase ID
  */
class FortressForwarderSerializerActor(phaseId : String) extends AbstractSerializerActor(phaseId : String)  {

  /**
    * The buffer that will hold multiple documents, when buffer_size > 0
    */
  var buffer : ListBuffer[Map[String,Any]] = new ListBuffer[Map[String,Any]]

  /**
    * The buffer size
    */
  var bufferSize : Int = -1

  /**
    * The extra headers which need to be attached to the outbound HTTP request
    */
  var headers : Map[String,Any] = Map.empty[String,Any]

  /**
    * The URL the outbound request should be sent to
    */
  var url : String = null

  override def receive: Receive = {
    case msg: WebParsedResponseMessage =>
      loadConfig(getPhase(msg))
      if(shouldCapture(msg)) {
        val exportableObject = AfthemResponseSerializer.toExportableObject(msg, discardRequestHeaders, discardResponseHeaders)
        if (bufferSize > 1)
          buffer += exportableObject
        if (bufferSize <= 1) {
          log.debug("Buffer size is -1, forwarding single document")
          performRequest(Parsers.serializeAsJsonString(exportableObject, pretty = false))
        } else {
          if (buffer.size >= bufferSize) {
            log.debug("Max buffer size reached, forwarding " + buffer.size + " documents")
            performRequest(Parsers.serializeAsJsonString(buffer.toArray, pretty = false))
            buffer = new ListBuffer[Map[String, Any]]
          }
        }
      }
  }

  /**
    * Loads the configuration from the phase
    * @param phase the phase
    */
  override def loadConfig(phase : Phase) = {
    super.loadConfig(phase)
    bufferSize = phase.getConfigInt("buffer_size",-1)
    headers = phase.getConfigMap("headers")
    url = phase.getConfigString("url",null)
  }


  /**
    * Performs the outbound HTTP request
    * @param body the serialized conversation to be forwarded
    */
  def performRequest(body : String) : Unit = {
    val post = new HttpPost(url)
    for ((k,v) <- headers) post.setHeader(k,v.asInstanceOf[String])
    post.setHeader("content-type","application/json")
    post.setEntity(new StringEntity(body))
    AfthemHttpClient.execute(post,new FutureCallback[HttpResponse] {
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

