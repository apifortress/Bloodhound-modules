package com.apifortress.afthem.modules.mongodb

import java.util

import com.mongodb.MongoCredential
import org.mongodb.scala.{MongoClient, MongoClientSettings, ServerAddress}

object MongoDbClientHelper {

  def create(host : String, port : Int, username : String = null,
             password : String = null, adminDatabase : String = null): MongoClient ={
    var settings = MongoClientSettings.builder()
      .applyToClusterSettings(builder =>
        builder.hosts(util.Arrays.asList(new ServerAddress(host, port)))
      ).codecRegistry(MongoClient.DEFAULT_CODEC_REGISTRY)

    if(username != null)
      settings = settings.credential(
        MongoCredential.createCredential(username, adminDatabase, password.toCharArray))
    return MongoClient(settings.build())
  }
}
