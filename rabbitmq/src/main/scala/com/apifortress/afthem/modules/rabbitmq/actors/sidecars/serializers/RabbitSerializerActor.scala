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

import com.apifortress.afthem.AfthemResponseSerializer
import com.apifortress.afthem.actors.sidecars.serializers.AbstractSerializerActor
import com.apifortress.afthem.config.Phase
import com.apifortress.afthem.messages.WebParsedResponseMessage
import com.rabbitmq.client.AMQP.BasicProperties
import com.rabbitmq.client.{Channel, Connection, ConnectionFactory}

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


  override def receive: Receive = {
    case msg : WebParsedResponseMessage =>
      loadConfig(getPhase(msg))
      if(shouldCapture(msg)){
        val data = AfthemResponseSerializer.serialize(msg,discardRequestHeaders,discardResponseHeaders)
        val properties = new BasicProperties.Builder().contentType("application/json").build()
        channel.basicPublish(exchangeName,routingKey, properties, data.getBytes())
      }
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
  }

  override def postStop(): Unit = {
    super.postStop()
    channel.close()
    connection.close()
  }

}
