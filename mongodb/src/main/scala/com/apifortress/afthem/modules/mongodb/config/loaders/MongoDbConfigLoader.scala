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
package com.apifortress.afthem.modules.mongodb.config.loaders

import com.apifortress.afthem.Parsers
import com.apifortress.afthem.config._
import com.apifortress.afthem.modules.mongodb.MongoDbClientHelper
import com.mongodb.client.model.Filters
import org.mongodb.scala.bson.BsonDocument
import org.mongodb.scala.bson.collection.immutable.Document

import scala.concurrent.Await
import scala.concurrent.duration.Duration

/**
  * Loader for configuration stored in MongoDB
  * @param params MongoDB configuration params
  */
class MongoDbConfigLoader(params: Map[String,Any] = null) extends TConfigLoader {

  /**
    * MongoDB client.
    */
  val client = MongoDbClientHelper.create(params("uri").asInstanceOf[String])
  val db = client.getDatabase("afthem")
  db.createCollection(params.getOrElse("collection","configuration").asInstanceOf[String])
  val collection = db.getCollection(params.getOrElse("collection","configuration").asInstanceOf[String])

  override def loadBackends(): Backends = {
    val iterable = collection.find(Filters.eq("type","backend"))
    val blist = Await.result(iterable.toFuture(),Duration.Inf).asInstanceOf[List[Document]].map(item => Parsers.parseJSON(item.toJson(),classOf[Backend]))
    return new Backends(blist)

  }

  override def loadFlow(id: String): Flow = {
    val first = collection.find(Filters.and(Filters.eq("type","flow"),Filters.eq("id",id))).first()
    return Parsers.parseJSON(Await.result(first.toFuture(),Duration.Inf).get("flow").get.asInstanceOf[BsonDocument].toJson(),classOf[Flow])
  }

  override def loadImplementers(): Implementers = {
    val first = collection.find(Filters.eq("type","implementers")).first()
    return Parsers.parseJSON(Await.result(first.toFuture(),Duration.Inf).toJson(),classOf[Implementers])
  }


}
