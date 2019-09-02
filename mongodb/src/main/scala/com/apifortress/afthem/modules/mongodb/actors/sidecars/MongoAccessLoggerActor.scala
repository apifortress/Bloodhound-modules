package com.apifortress.afthem.modules.mongodb.actors.sidecars

import com.apifortress.afthem.actors.AbstractAfthemActor
import com.apifortress.afthem.config.Phase
import com.apifortress.afthem.messages.{WebParsedRequestMessage, WebParsedResponseMessage}
import com.apifortress.afthem.modules.mongodb.actors.TMongoDBActor
import org.bson.Document

import scala.collection.mutable.ListBuffer

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
    case msg : WebParsedRequestMessage => {
      initClient(getPhase(msg))
      val document = new Document("type","inbound")
      document.put("remoteIP",msg.request.remoteIP)
      document.put("method",msg.request.method)
      document.put("url",msg.request.getURL())
      insert(document)
    }
    case msg : WebParsedResponseMessage => {
      initClient(getPhase(msg))
      val document = new Document("type","outbound")
      document.put("status",msg.response.status)
      document.put("url",msg.response.getURL())
      document.put("method",msg.response.method)
      insert(document)
    }
  }

  override def initClient(phase : Phase) = {
    super.initClient(phase)
    bufferSize = phase.getConfigInt("buffer_size",-1)
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
