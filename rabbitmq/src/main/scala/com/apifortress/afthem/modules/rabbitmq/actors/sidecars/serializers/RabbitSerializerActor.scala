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
package com.apifortress.afthem.modules.rabbitmq.actors.sidecars.serializers

import com.apifortress.afthem.{AfthemResponseSerializer, Metric}
import com.apifortress.afthem.actors.sidecars.serializers.AbstractSerializerActor
import com.apifortress.afthem.config.Phase
import com.apifortress.afthem.messages.WebParsedResponseMessage
import com.rabbitmq.client.AMQP.BasicProperties
import com.rabbitmq.client.{Channel, Connection, ConnectionFactory}
import collection.JavaConverters._


/**
  * Serializes the API conversation in the API Fortress-compatible format and publish it to a RabbitMQ exchange
  * @param phaseId the ID of the phase
  */
class RabbitSerializerActor(phaseId : String) extends AbstractSerializerActor(phaseId : String) {

  /**
    * the AMQP connection
    */
  var connection : Connection = null
  /**
    * The AMQP channel
    */
  var channel : Channel = null
  /**
    * The exchange name
    */
  var exchangeName : String = null
  /**
    * The routing key
    */
  var routingKey : String = null

  var headers : Map[String,Any] = null

  var ttl : Int = -1


  override def receive: Receive = {
    case msg : WebParsedResponseMessage =>
      loadConfig(getPhase(msg))
      val m = new Metric()
      if(shouldCapture(msg)){
        val data = AfthemResponseSerializer.serialize(msg,discardRequestHeaders,discardResponseHeaders)
        var builder = new BasicProperties.Builder().contentType("application/json")
        if (ttl > -1)
          builder = builder.expiration(String.valueOf(ttl))
        if (headers != null) {
          val data = headers.map(it => (it._1->it._2.asInstanceOf[AnyRef])).asJava
          builder = builder.headers(data)
        }

        val properties = builder.build()
        channel.basicPublish(exchangeName,routingKey, properties, data.getBytes())
      }
      metricsLog.debug(m.toString())
  }

  override def loadConfig(phase: Phase): Unit = {
    super.loadConfig(phase)
    if(channel  == null) {
      val factory = new ConnectionFactory()
      factory.setUri(phase.getConfigString("uri", null))
      connection = factory.newConnection()
      channel = connection.createChannel()
    }
    exchangeName = phase.getConfigString("exchange", null)
    routingKey = phase.getConfigString("routing_key","")
    headers = phase.getConfigMap("headers")
    ttl = phase.getConfigInt("ttl",-1)
  }

  override def postStop(): Unit = {
    super.postStop()
    if(channel != null) {
      channel.close()
      channel = null
    }
    if(connection != null) {
      connection.close()
      connection = null
    }
  }

}