package com.apifortress.afthem.modules.mongodb.actors.sidecars.serializers

import java.util

import com.apifortress.afthem.AfthemResponseSerializer
import com.apifortress.afthem.actors.AbstractAfthemActor
import com.apifortress.afthem.config.Phase
import com.apifortress.afthem.messages.WebParsedResponseMessage
import org.mongodb.scala.bson.BsonTransformer
import org.mongodb.scala.bson.collection.mutable.Document
import org.mongodb.scala.{Completed, MongoClient, MongoClientSettings, MongoCollection, Observer, ServerAddress}

import scala.collection.mutable.ListBuffer


class MongoSerializerActor(phaseId : String) extends AbstractAfthemActor(phaseId : String) {

  var client : MongoClient = null
  var collection : MongoCollection[Document] = null

  var buffer : ListBuffer[Document] = new ListBuffer[Document]

  var bufferSize : Int = -1

  var extraFields : Map[String,Any] = null

  override def receive: Receive = {
    case msg: WebParsedResponseMessage =>
        initClient(getPhase(msg))
        val exportableObject = AfthemResponseSerializer.serialize(msg)
        val document = Document(exportableObject)
        applyExtraFields(document)
        if(bufferSize > -1)
          buffer+=document
        if(bufferSize == -1) {
          log.debug("Buffer size is -1, inserting single document")
          collection.insertOne(document).subscribe(new Observer[Completed] {
            override def onNext(result: Completed): Unit = {}

            override def onError(e: Throwable) = {
              log.error("Cannot save single document to MongoDB",e)
            }
            override def onComplete(): Unit = {}
          })
        } else {
          if(buffer.size >= bufferSize)
           insertBufferedDocuments
        }
  }

  override def postStop(): Unit = {
    super.postStop()
    if(buffer.size > 0) {
      log.debug("Buffer is dirty. Saving items to MongoDB")
      insertBufferedDocuments
    }
  }

  def insertBufferedDocuments : Unit = {
    log.debug("Saving buffered documents to MongoDB")
    val localBuffer = buffer
    buffer = new ListBuffer[Document]
    collection.insertMany(localBuffer).subscribe(new Observer[Completed] {
      override def onNext(result: Completed): Unit = {}

      override def onError(e: Throwable): Unit = {
        log.error("Cannot save multiple documents to MongoDB",e)
      }

      override def onComplete(): Unit = {}
    })
  }

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

  def initClient(phase : Phase) = {
    bufferSize = phase.getConfigInt("buffer_size",-1)
    extraFields = phase.getConfigMap("extra_fields")
    if(client == null) {
      val settings = MongoClientSettings.builder()
        .applyToClusterSettings(builder =>
          builder.hosts(util.Arrays.asList(new ServerAddress(phase.getConfigString("host"), phase.getConfigInt("port"))))
        )
        .codecRegistry(MongoClient.DEFAULT_CODEC_REGISTRY)
        .build()
      client = MongoClient(settings)
      val db = client.getDatabase(phase.getConfigString("database"))
      db.createCollection(phase.getConfigString("collection"))
      collection = db.getCollection(phase.getConfigString("collection"))
    }
  }
}