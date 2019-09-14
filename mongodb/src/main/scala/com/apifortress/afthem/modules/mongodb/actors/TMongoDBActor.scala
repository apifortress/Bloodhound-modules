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
import com.mongodb.ConnectionString
import org.bson.Document
import org.mongodb.scala.{Completed, MongoClient, MongoClientSettings, MongoCollection, Observer}
import org.slf4j.Logger

import scala.collection.mutable.ListBuffer

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

  def getLog() : Logger

  /**
    * Initializes the MongoClient, if the client hasn't been initialized already
    * @param phase the phase
    */
  def initClient(phase : Phase) : Unit = {
    initClient(phase.getConfigString("uri"),
                phase.getConfigString("database"),
                phase.getConfigString("collection"))
  }

  /**
    * Initializes the MongoClient, if the client hasn't been initialized already
    * @param uri the URI of the MongoDB server
    * @param database the database to be used
    * @param collection the collection to be used
    */
  def initClient(uri : String, database : String = "afthem", collection : String = "configuration") : Unit = {
    if(client == null) {
      client = MongoClient(MongoClientSettings.builder()
                            .applyConnectionString(new ConnectionString(uri))
                            .codecRegistry(MongoClient.DEFAULT_CODEC_REGISTRY).build())
      val db = client.getDatabase(database)
      db.createCollection(collection)
      this.collection = db.getCollection(collection)
    }
  }

  /**
    * Inserts a document
    * @param document a document
    */
  def insertSingleDocument(document : Document) = {
    getLog.debug("Buffer size is -1, inserting single document")
    collection.insertOne(document).subscribe(new Observer[Completed] {
      override def onNext(result: Completed): Unit = {}

      override def onError(e: Throwable) = {
        getLog.error("Cannot save single document to MongoDB", e)
      }

      override def onComplete(): Unit = {
        getLog.debug("Document saved to MongoDB")
      }
    })
  }

  /**
    * Inserts a buffer of documents
    * @param documents a buffer of documents
    */
  def insertManyDocuments(documents : ListBuffer[Document]) = {
    collection.insertMany(documents).subscribe(new Observer[Completed] {
      override def onNext(result: Completed): Unit = {}

      override def onError(e: Throwable): Unit = {
        getLog.error("Cannot save multiple documents to MongoDB",e)
      }
      override def onComplete(): Unit = {
        getLog.debug("Documents saved to MongoDB")
      }
    })
  }

}
