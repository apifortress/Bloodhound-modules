package com.apifortress.afthem.modules.redis.actors.proxy

import com.apifortress.afthem.actors.proxy.AbstractUpstreamActor
import com.apifortress.afthem.config.Phase
import com.apifortress.afthem.exceptions.{AfthemFlowException, AfthemSevereException}
import com.apifortress.afthem.messages.beans.{Header, HttpWrapper}
import com.apifortress.afthem.messages.{ExceptionMessage, WebParsedRequestMessage, WebParsedResponseMessage}
import com.apifortress.afthem.{Parsers, ReqResUtil}
import redis.clients.jedis.Jedis
import redis.clients.jedis.exceptions.JedisDataException
import redis.clients.jedis.params.SetParams

class UpstreamRedisActor(phaseId : String) extends AbstractUpstreamActor(phaseId) {

  var connection : Jedis = _

  val okMessage = "{\"status\":\"ok\"}".getBytes

  override def receive: Receive = {
    case msg : WebParsedRequestMessage =>
      logUpstreamMetrics(msg)
      try {
        loadConfig(getPhase(msg))
      }catch {
        case e : Exception =>
          throw new AfthemSevereException(msg,e.getMessage)
      }
      val op = if (msg.request.containsHeader("x-op")) msg.request.getHeader("x-op") else "get"
      try {
        val inputData = Parsers.parseJSON(msg.request.payload, classOf[Map[String,Any]])
        val params = inputData.getOrElse("params",Map.empty[String,Any]).asInstanceOf[Map[String,Any]]
        val expire = params.getOrElse("expire",-1).asInstanceOf[Int]
        op match {
          case "set" =>
            connection.set(inputData("key").toString, inputData("value").toString, SetParams.setParams().ex(expire))
            forward(buildOkMessage(msg))
          case "zadd" =>
            connection.zadd(inputData("key").toString, inputData("score").asInstanceOf[Double], inputData("value").toString)
            forward(buildOkMessage(msg))
          case "sadd" =>
            connection.sadd(inputData("key").toString, inputData("value").toString)
            forward(buildOkMessage(msg))
          case "spop" =>
            val count = inputData.getOrElse("count", 1).asInstanceOf[Int]
            val data = if (count == 1) Parsers.serializeAsJsonByteArray(Map("value" -> connection.spop(inputData("key").toString)))
            else Parsers.serializeAsJsonByteArray(Map("value" -> connection.spop(inputData("key").toString, count)))
            forward(new WebParsedResponseMessage(buildResponseWrapper(msg.request, data), msg))
          case "hset" =>
            connection.hset(inputData("key").toString, inputData("field").toString, inputData("value").toString)
            forward(buildOkMessage(msg))
          case "smembers" =>
            val data = Parsers.serializeAsJsonByteArray(Map("value" -> connection.smembers(inputData("key").toString)))
            forward(buildOkMessage(msg))
          case "hget" =>
            val data = Parsers.serializeAsJsonByteArray(Map("value" -> connection.hget(inputData("key").toString, inputData("field").toString)))
            forward(new WebParsedResponseMessage(buildResponseWrapper(msg.request, data), msg))
          case "hgetall" =>
            val data = Parsers.serializeAsJsonByteArray(Map("value" -> connection.hgetAll(inputData("key").toString)))
            forward(new WebParsedResponseMessage(buildResponseWrapper(msg.request, data), msg))
          case "expire" =>
            connection.expire(inputData("key").toString,inputData("value").asInstanceOf[Int])
            forward(buildOkMessage(msg))
          case _ =>
            val data = Parsers.serializeAsJsonByteArray(Map("value" -> connection.get(inputData("key").toString)))
            forward(new WebParsedResponseMessage(buildResponseWrapper(msg.request, data), msg))
        }
      }catch {
        case e : JedisDataException =>
            new ExceptionMessage(e,400,msg).respond(ReqResUtil.MIME_JSON)
        case e : Exception =>
          throw new AfthemFlowException(msg,e.getMessage)
      }
  }

  def loadConfig(phase : Phase) : Unit = {
    if(connection == null)
      connection = new Jedis(phase.getConfigString("uri"))
  }

  override def postStop(): Unit = {
    if (connection != null)
      connection.close()
    connection = null
  }

  def buildResponseWrapper(requestWrapper : HttpWrapper, data : Array[Byte]) : HttpWrapper = {
    new HttpWrapper(requestWrapper.getURL(), ReqResUtil.STATUS_OK, requestWrapper.method,
      List(new Header("Content-Type","application/json")), data, requestWrapper.remoteIP, ReqResUtil.CHARSET_UTF8)
  }
  def buildOkMessage(requestMessage: WebParsedRequestMessage) : WebParsedResponseMessage ={
    val wrapper = new HttpWrapper(requestMessage.request.getURL(), ReqResUtil.STATUS_OK, requestMessage.request.method,
      List(new Header("Content-Type","application/json")), okMessage, requestMessage.request.remoteIP, ReqResUtil.CHARSET_UTF8)
    new WebParsedResponseMessage(wrapper,requestMessage)
  }

}

