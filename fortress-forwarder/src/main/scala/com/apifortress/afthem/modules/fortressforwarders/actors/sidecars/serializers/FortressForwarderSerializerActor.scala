package com.apifortress.afthem.modules.fortressforwarders.actors.sidecars.serializers

import com.apifortress.afthem.actors.AbstractAfthemActor
import com.apifortress.afthem.config.Phase
import com.apifortress.afthem.messages.WebParsedResponseMessage
import com.apifortress.afthem.{AfthemResponseSerializer, Parsers}
import com.fasterxml.jackson.databind.ObjectMapper
import org.apache.http.client.HttpClient
import org.apache.http.client.methods.HttpPost
import org.apache.http.entity.StringEntity
import org.apache.http.impl.client.HttpClients
import org.apache.http.util.EntityUtils

import scala.collection.mutable.ListBuffer

class FortressForwarderSerializerActor(phaseId : String) extends AbstractAfthemActor(phaseId : String)  {

  var buffer : ListBuffer[Map[String,Any]] = new ListBuffer[Map[String,Any]]

  var bufferSize : Int = -1

  var headers : Map[String,Any] = Map.empty[String,Any]

  val httpClient : HttpClient = HttpClients.createDefault()

  val objectMapper : ObjectMapper = new ObjectMapper()

  var url : String = null

  var discardRequestHeaders : List[String] = null

  var discardResponseHeaders : List[String] = null

  override def receive: Receive = {
    case msg: WebParsedResponseMessage =>
      loadConfig(getPhase(msg))
      val exportableObject = AfthemResponseSerializer.toExportableObject(msg,discardRequestHeaders, discardResponseHeaders)
      if(bufferSize > -1)
        buffer+=exportableObject
      if(bufferSize == -1) {
        log.debug("Buffer size is -1, inserting single document")
        performRequest(Parsers.serializeAsJsonString(exportableObject, pretty = false))
      } else {
        if(buffer.size >= bufferSize) {
          performRequest(Parsers.serializeAsJsonString(buffer.toArray, pretty = false))
          buffer = new ListBuffer[Map[String,Any]]
        }
      }
  }

  def loadConfig(phase : Phase) = {
    bufferSize = phase.getConfigInt("buffer_size",-1)
    headers = phase.getConfigMap("headers")
    url = phase.getConfigString("url",null)
    discardRequestHeaders = phase.getConfigList("discard_request_headers")
    discardResponseHeaders = phase.getConfigList("discard_response_headers")
  }


  def performRequest(body : String) : Unit = {
    val post = new HttpPost(url)
    for ((k,v) <- headers) post.setHeader(k,v.asInstanceOf[String])
    post.setHeader("content-type","application/json")
    post.setEntity(new StringEntity(body))
    val response = httpClient.execute(post)
    val respEntity = response.getEntity
    EntityUtils.consumeQuietly(respEntity)
  }

}
