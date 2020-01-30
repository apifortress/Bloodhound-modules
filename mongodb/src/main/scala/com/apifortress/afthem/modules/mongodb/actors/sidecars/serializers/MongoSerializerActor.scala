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
package com.apifortress.afthem.modules.mongodb.actors.sidecars.serializers

import com.apifortress.afthem.AfthemResponseSerializer
import com.apifortress.afthem.actors.sidecars.serializers.AbstractSerializerActor
import com.apifortress.afthem.config.Phase
import com.apifortress.afthem.messages.WebParsedResponseMessage
import com.apifortress.afthem.modules.mongodb.actors.TMongoDBActor
import org.bson.Document
import org.mongodb.scala.bson.BsonTransformer

import scala.collection.mutable.ListBuffer

/**
  * Serializes the API conversation in the API Fortress-compatible format and stores it into MongoDB
  * @param phaseId
  */
class MongoSerializerActor(phaseId : String) extends AbstractSerializerActor(phaseId : String) with TMongoDBActor {

  /**
    * If buffering is activated, this list will store a certain amount of documents waiting to be inserted
    */
  var buffer : ListBuffer[Document] = new ListBuffer[Document]

  /**
    * The size of the buffer. -1 means "don't buffer"
    */
  var bufferSize : Int = -1

  /**
    * Extra fields that need to be stored in the document
    */
  var extraFields : Map[String,Any] = null

  override def receive: Receive = {
    case msg: WebParsedResponseMessage =>
      val phase = getPhase(msg)
      loadConfig(phase)
      initClient(phase)
      if(shouldCapture(msg)) {
        val exportableObject = AfthemResponseSerializer.serialize(msg,discardRequestHeaders,discardResponseHeaders)
        val document = Document.parse(exportableObject)
        applyExtraFields(document)
        if (bufferSize > 1)
          buffer += document
        if (bufferSize <= 1) {
          log.debug("Inserting single document")
          insertSingleDocument(document)
        } else {
          if (buffer.size >= bufferSize)
            insertBufferedDocuments
        }
      }
  }

  override def postStop(): Unit = {
    super.postStop()
    if(buffer.size > 0) {
      log.debug("Buffer is full. Saving items to MongoDB")
      insertBufferedDocuments
    }
    client.close()
  }

  /**
    * Inserts the buffered documents in MongoDB
    */
  def insertBufferedDocuments : Unit = {
    log.debug("Saving buffered documents to MongoDB")
    val localBuffer = buffer
    buffer = new ListBuffer[Document]
    insertManyDocuments(localBuffer)
  }

  /**
    * Applies extra fields to a document
    * @param document
    */
  def applyExtraFields(document : Document) = {
    if (extraFields.size > 0){
      extraFields.map{ item =>
        item._2 match {
          case Int => (item._1,BsonTransformer.TransformInt(item._2.asInstanceOf[Int]))
          case Double => (item._1,BsonTransformer.TransformDouble(item._2.asInstanceOf[Double]))
          case Boolean => (item._1,BsonTransformer.TransformBoolean(item._2.asInstanceOf[Boolean]))
          case _ => (item._1,BsonTransformer.TransformString(item._2.asInstanceOf[String]))
        }
      }.foreach( item => document.put(item._1,item._2))
    }
  }

  /**
    * Initizalize the MongoDB client
    * @param phase the phase
    */
  override def initClient(phase : Phase) = {
    bufferSize = phase.getConfigInt("buffer_size",-1)
    extraFields = phase.getConfigMap("extra_fields")
    super.initClient(phase)
  }

}