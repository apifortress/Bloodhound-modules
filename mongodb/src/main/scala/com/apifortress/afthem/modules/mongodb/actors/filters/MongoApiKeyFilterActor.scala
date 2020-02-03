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
package com.apifortress.afthem.modules.mongodb.actors.filters

import com.apifortress.afthem.actors.filters.ApiKeyFilterActor
import com.apifortress.afthem.config.{ApiKey, Phase}
import com.apifortress.afthem.exceptions.AfthemSevereException
import com.apifortress.afthem.messages.WebParsedRequestMessage
import com.apifortress.afthem.modules.mongodb.actors.TMongoDBActor
import com.mongodb.client.model.Filters

import scala.concurrent.Await
import scala.concurrent.duration.Duration

/**
  * Actor to filter requests based on an API key. API keys are stored in MongoDB
  * @param phaseId
  */
class MongoApiKeyFilterActor(phaseId : String) extends ApiKeyFilterActor(phaseId : String) with TMongoDBActor {

  override def receive: Receive = {
    case msg: WebParsedRequestMessage =>
      try {
        initClient(getPhase(msg))
        super.receive(msg)
      }catch {
        case e : Exception => throw new AfthemSevereException(msg,e.getMessage)
      }
  }

  override def findKey(key: String, phase: Phase): Option[ApiKey] = {
    val future = collection.find(Filters.eq("api_key",key)).first().toFuture()
    val document = Await.result(future,Duration.Inf)
    if(document == null)
      return None
    return Some(new ApiKey(apiKey = document.getString("api_key"), appId = document.getString("app_id"), enabled = document.getBoolean("enabled")))
  }

  override def postStop() : Unit = {
    if(client != null) {
      log.debug("Stopping MongoDB client")
      client.close()
    }
  }

}
