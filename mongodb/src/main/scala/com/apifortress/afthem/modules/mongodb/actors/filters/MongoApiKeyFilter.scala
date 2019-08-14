package com.apifortress.afthem.modules.mongodb.actors.filters

import com.apifortress.afthem.actors.filters.ApiKeyFilter
import com.apifortress.afthem.config.{ApiKey, Phase}
import com.apifortress.afthem.messages.{WebParsedRequestMessage, WebParsedResponseMessage}
import com.apifortress.afthem.modules.mongodb.MongoDbClientHelper
import com.mongodb.client.model.Filters
import org.bson.Document
import org.mongodb.scala.{MongoClient, MongoCollection}

import scala.concurrent.Await
import scala.concurrent.duration.Duration

class MongoApiKeyFilter(phaseId : String) extends ApiKeyFilter(phaseId : String) {


  private var client : MongoClient = null

  private var collection : MongoCollection[Document] = null


  override def receive: Receive = {
    case msg: WebParsedRequestMessage =>
      initClient(getPhase(msg))
      super.receive(msg)
  }

  def initClient(phase : Phase) = {
    if(client == null) {
      client = MongoDbClientHelper.create(phase.getConfigString("uri"))
      val db = client.getDatabase(phase.getConfigString("database"))
      db.createCollection(phase.getConfigString("collection"))
      collection = db.getCollection(phase.getConfigString("collection"))
    }
  }

  override def findKey(key: String, phase: Phase): Option[ApiKey] = {
    val future = collection.find(Filters.eq("api_key",key)).first().toFuture()
    val document = Await.result(future,Duration.Inf)
    if(document == null)
      return None
    return Some(new ApiKey(apiKey = document.getString("api_key"), appId = document.getString("app_id"), enabled = document.getBoolean("enabled")))
  }

}
