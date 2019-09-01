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
package com.apifortress.afthem.modules.mongodb

import com.mongodb.ConnectionString
import org.mongodb.scala.{MongoClient, MongoClientSettings}

/**
  * Simplifies the creation of a MongoDB connection
  */
object MongoDbClientHelper {

  /**
    * Creates a MongoClient
    * @param uri the URI of the MongoDB instance
    * @return the MongoClient
    */
  def create(uri : String): MongoClient = {
    return MongoClient(MongoClientSettings.builder()
                              .applyConnectionString(new ConnectionString(uri))
                                  .codecRegistry(MongoClient.DEFAULT_CODEC_REGISTRY).build())
  }
}
