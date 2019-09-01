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
package com.apifortress.afthem.modules.mongodb.actors

import com.apifortress.afthem.config.Phase
import com.apifortress.afthem.modules.mongodb.MongoDbClientHelper
import org.bson.Document
import org.mongodb.scala.{MongoClient, MongoCollection}

/**
  * Common trait for all MongoDB Actors
  */
trait TMongoDBActor {

  /**
    * The MongoDB client
    */
  protected var client : MongoClient = null

  /**
    * The Collection that stores the API keys
    */
  protected var collection : MongoCollection[Document] = null

  def initClient(phase : Phase) = {
    if(client == null) {
      client = MongoDbClientHelper.create(phase.getConfigString("uri"))
      val db = client.getDatabase(phase.getConfigString("database"))
      db.createCollection(phase.getConfigString("collection"))
      collection = db.getCollection(phase.getConfigString("collection"))
    }
  }

}
