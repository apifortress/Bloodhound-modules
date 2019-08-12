package com.apifortress.afthem.modules.mongodb

import java.util

import com.mongodb.{ConnectionString, MongoCredential}
import org.mongodb.scala.{MongoClient, MongoClientSettings, ServerAddress}

/**
  * Simplifies the creation of a MongoDB connection
  */
object MongoDbClientHelper {

  def create(uri : String): MongoClient = {
    return MongoClient(MongoClientSettings.builder()
                              .applyConnectionString(new ConnectionString(uri))
                                  .codecRegistry(MongoClient.DEFAULT_CODEC_REGISTRY).build())
  }
}
