/**
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
package com.apifortress.afthem.modules.mongodb.actors.proxy

import com.apifortress.afthem.ReqResUtil
import com.apifortress.afthem.actors.AbstractAfthemActor
import com.apifortress.afthem.config.Phase
import com.apifortress.afthem.exceptions.AfthemSevereException
import com.apifortress.afthem.messages.beans.{Header, HttpWrapper}
import com.apifortress.afthem.messages.{ExceptionMessage, WebParsedRequestMessage, WebParsedResponseMessage}
import com.apifortress.afthem.modules.mongodb.actors.TMongoDBActor
import org.bson
import org.bson.BsonInvalidOperationException
import org.bson.json.JsonParseException
import org.mongodb.scala.bson.collection.immutable.Document
import org.mongodb.scala.result.DeleteResult
import org.mongodb.scala.{Completed, Observer}

object UpstreamMongoActor {

  val OPERATION_FIND = "find"
  val OPERATION_INSERT = "insert"
  val OPERATION_DELETE = "delete"
  val HEADER_LIMIT = "x-limit-results"
  val HEADER_OP = "x-op"
  val ATTR_MAX_DOCUMENTS = "max_documents"
}

/**
  * Upstream connecting to a MongoDB database
  * @param phaseId the phase ID
  */
class UpstreamMongoActor(override val phaseId: String) extends AbstractAfthemActor(phaseId) with TMongoDBActor {

  /**
    * Max number of documents default
    */
  private var maxDocuments : Int = 100

  override def receive: Receive = {
    case msg: WebParsedRequestMessage =>
      try {
        initClient(getPhase(msg))
        val limit = if (msg.request.containsHeader(UpstreamMongoActor.HEADER_LIMIT))
                      msg.request.getHeader(UpstreamMongoActor.HEADER_LIMIT).toInt
                    else
                      maxDocuments

        val op = if (msg.request.containsHeader(UpstreamMongoActor.HEADER_OP))
                      msg.request.getHeader(UpstreamMongoActor.HEADER_OP)
                    else
                      UpstreamMongoActor.OPERATION_FIND

        op match {
          case UpstreamMongoActor.OPERATION_FIND =>
                var query = collection.find(Document.apply(msg.request.getPayloadAsText()))
                if (limit > -1)
                  query = query.limit(limit)
                query.subscribe(new DocCollectorObserver(msg))
          case UpstreamMongoActor.OPERATION_INSERT =>
                collection.insertOne(Document(msg.request.getPayloadAsText())).subscribe(new UpdateObserver(msg))
          case UpstreamMongoActor.OPERATION_DELETE =>
                collection.deleteMany(Document(msg.request.getPayloadAsText())).subscribe(new DeleteObserver(msg))
        }

      }catch{
        case e : JsonParseException =>
          new ExceptionMessage(e,ReqResUtil.STATUS_BAD_REQUEST,msg).respond(ReqResUtil.MIME_JSON)
        case e: BsonInvalidOperationException =>
          new ExceptionMessage(e,ReqResUtil.STATUS_BAD_REQUEST,msg).respond(ReqResUtil.MIME_JSON)
        case e : Exception =>
          throw new AfthemSevereException(msg,e.getMessage)
      }
  }

  override def initClient(phase: Phase): Unit = {
    super.initClient(phase)
    maxDocuments = phase.getConfigInt(UpstreamMongoActor.ATTR_MAX_DOCUMENTS,100)
  }

  override def postStop() : Unit = {
    if(client != null) {
      log.debug("Stopping MongoDB client")
      client.close()
    }
  }

  /**
    * Observer collecting documents to send back
    * @param msg the request message
    */
  class DocCollectorObserver(val msg: WebParsedRequestMessage) extends Observer[bson.Document] {

    /**
      * StringBuilder to collect the documents in form of string
      */
    private val collector = new StringBuilder()

    collector.append("[\n")

    private var firstIteration = true

    override def onNext(result: bson.Document): Unit = {
      if(firstIteration)
        firstIteration = false
      else
        collector.append(",\n")
      collector.append(result.toJson)
    }

    override def onError(e: Throwable): Unit = {
      new ExceptionMessage(e.asInstanceOf[Exception],ReqResUtil.STATUS_INTERNAL,msg).respond(ReqResUtil.MIME_JSON)
    }

    override def onComplete(): Unit = {
      collector.append("\n]")
      val wrapper = new HttpWrapper(msg.request.getURL(),ReqResUtil.STATUS_OK,msg.request.method,
                                    List(new Header(ReqResUtil.HEADER_CONTENT_TYPE,ReqResUtil.MIME_JSON)),
                                    collector.toString().getBytes(),
                                    msg.request.remoteIP, ReqResUtil.CHARSET_UTF8)
      val response = new WebParsedResponseMessage(wrapper,msg.request,msg.backend,msg.flow,
                                                  msg.deferredResult,msg.date,msg.meta)
      forward(response)
    }
  }

  /**
    * Observer to respond upon insert/update conclusion
    * @param msg the request message
    */
  class UpdateObserver(val msg: WebParsedRequestMessage) extends Observer[Completed] {

    /**
      * Success message
      */
    private val success = "{\"status\":\"ok\"}".getBytes

    override def onNext(result: Completed): Unit = {
      val wrapper = new HttpWrapper(msg.request.getURL(),ReqResUtil.STATUS_OK,msg.request.method,
                                    List(new Header(ReqResUtil.HEADER_CONTENT_TYPE, ReqResUtil.MIME_JSON)),
                                    success,
                                    msg.request.remoteIP,ReqResUtil.CHARSET_UTF8)
      val response = new WebParsedResponseMessage(wrapper,msg.request,msg.backend,msg.flow,
                                                  msg.deferredResult,msg.date,msg.meta)
      forward(response)
    }

    override def onError(e: Throwable): Unit = {
      new ExceptionMessage(e.asInstanceOf[Exception],ReqResUtil.STATUS_INTERNAL,msg).respond(ReqResUtil.MIME_JSON)
    }

    override def onComplete(): Unit = {}
  }

  class DeleteObserver(val msg: WebParsedRequestMessage) extends Observer[DeleteResult] {
    override def onNext(result: DeleteResult): Unit = {
      val wrapper = new HttpWrapper(msg.request.getURL(),ReqResUtil.STATUS_OK,msg.request.method,
                                      List(new Header(ReqResUtil.HEADER_CONTENT_TYPE, ReqResUtil.MIME_JSON)),
                                      s"""{"status":"ok", "deletedCount":${result.getDeletedCount}}""".getBytes,
                                      msg.request.remoteIP,ReqResUtil.CHARSET_UTF8)
      val response = new WebParsedResponseMessage(wrapper,msg.request,msg.backend,msg.flow,
                                                     msg.deferredResult,msg.date,msg.meta)
      forward(response)
    }

    override def onError(e: Throwable): Unit = {
      new ExceptionMessage(e.asInstanceOf[Exception],ReqResUtil.STATUS_INTERNAL,msg).respond(ReqResUtil.MIME_JSON)
    }

    override def onComplete(): Unit = {}
  }

}