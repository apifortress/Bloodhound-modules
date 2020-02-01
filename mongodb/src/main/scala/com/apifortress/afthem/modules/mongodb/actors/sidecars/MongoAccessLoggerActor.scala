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
package com.apifortress.afthem.modules.mongodb.actors.sidecars

import com.apifortress.afthem.actors.AbstractAfthemActor
import com.apifortress.afthem.config.{ApiKey, Phase}
import com.apifortress.afthem.exceptions.{AfthemSevereException, RejectedRequestException, UnauthenticatedException, UnauthorizedException}
import com.apifortress.afthem.messages.{BaseMessage, ExceptionMessage, WebParsedRequestMessage, WebParsedResponseMessage}
import com.apifortress.afthem.modules.mongodb.actors.TMongoDBActor
import org.bson.Document

import scala.collection.mutable.ListBuffer

/**
  * Actor for logging accesses
  * @param phaseId the phase ID
  */
class MongoAccessLoggerActor(phaseId : String) extends AbstractAfthemActor(phaseId : String) with TMongoDBActor {

  /**
    * If buffering is activated, this list will store a certain amount of documents waiting to be inserted
    */
  var buffer : ListBuffer[Document] = new ListBuffer[Document]

  /**
    * The size of the buffer. -1 means "don't buffer"
    */
  var bufferSize : Int = -1

  override def receive: Receive = {
    case msg: ExceptionMessage if(msg.exception.isInstanceOf[RejectedRequestException]) =>
      try {
        initClient(getPhase(msg))
        val exception = msg.exception.asInstanceOf[RejectedRequestException]
        val document = new Document("type", "rejected")
        document.put("remoteIP", exception.message.request.remoteIP)
        document.put("method", exception.message.request.method)
        document.put("url", exception.message.request.getURL())
        insert(document)
      }catch {
        case e : Exception => throw new AfthemSevereException(msg,e.getMessage)
      }
    case msg: ExceptionMessage if(msg.exception.isInstanceOf[UnauthorizedException]) =>
      try {
        initClient(getPhase(msg))
        val exception = msg.exception.asInstanceOf[UnauthorizedException]
        val document = new Document("type","rejected")
        document.put("remoteIP",exception.message.request.remoteIP)
        document.put("method",exception.message.request.method)
        document.put("url",exception.message.request.getURL())
        insert(document)
      }catch {
        case e : Exception => throw new AfthemSevereException(msg,e.getMessage)
      }
    case msg: ExceptionMessage if(msg.exception.isInstanceOf[UnauthenticatedException]) =>
      try{
        initClient(getPhase(msg))
        val exception = msg.exception.asInstanceOf[UnauthenticatedException]
        val document = new Document("type","unauthenticated")
        document.put("remoteIP",exception.message.request.remoteIP)
        document.put("method",exception.message.request.method)
        document.put("url",exception.message.request.getURL())
        insert(document)
      }catch {
        case e : Exception => throw new AfthemSevereException(msg,e.getMessage)
      }
    case msg : WebParsedRequestMessage =>
      try{
        initClient(getPhase(msg))
        val document = new Document("type","inbound")
        document.put("remoteIP",msg.request.remoteIP)
        document.put("method",msg.request.method)
        document.put("url",msg.request.getURL())
        evaluateAdditionalFields(document,msg)
        insert(document)
      }catch {
        case e : Exception => throw new AfthemSevereException(msg,e.getMessage)
      }

    case msg : WebParsedResponseMessage =>
      try {
        initClient(getPhase(msg))
        val document = new Document("type","outbound")
        document.put("status",msg.response.status)
        document.put("url",msg.response.getURL())
        document.put("method",msg.response.method)
        evaluateAdditionalFields(document,msg)
        insert(document)
      }catch {
        case e : Exception => throw new AfthemSevereException(msg,e.getMessage)
      }
  }

  override def initClient(phase : Phase) = {
    super.initClient(phase)
    bufferSize = phase.getConfigInt("buffer_size",-1)
  }

  def evaluateAdditionalFields(document : Document, msg : BaseMessage) = {
    val keyOption = msg.meta.get("key")
    if(keyOption.isDefined)
      document.put("app_id",keyOption.get.asInstanceOf[ApiKey].appId)
  }

  override def postStop(): Unit = {
    super.postStop()
    if(buffer.size > 0) {
      log.debug("Buffer is full. Saving items to MongoDB")
      insertManyDocuments(buffer)
    }
  }

  def insert(document : Document) : Unit = {
    if(bufferSize > 1) {
      buffer += document
      if(buffer.size >= bufferSize) {
        log.debug("Saving buffered documents to MongoDB")
        val localBuffer = buffer
        buffer = new ListBuffer[Document]
        insertManyDocuments(localBuffer)
      }
    } else {
        log.debug("Buffer size is -1, inserting single document")
        insertSingleDocument(document)
    }
  }


}
