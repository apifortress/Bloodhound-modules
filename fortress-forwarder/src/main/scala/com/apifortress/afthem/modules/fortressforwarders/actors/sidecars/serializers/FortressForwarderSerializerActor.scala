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

/**
  * Actor that serializes an API conversation and sends over to an HTTP endpoint
  * @param phaseId the phase ID
  */
class FortressForwarderSerializerActor(phaseId : String) extends AbstractAfthemActor(phaseId : String)  {

  /**
    * The buffer that will hold multiple documents, when buffer_size > 0
    */
  var buffer : ListBuffer[Map[String,Any]] = new ListBuffer[Map[String,Any]]

  /**
    * The buffer size
    */
  var bufferSize : Int = -1

  /**
    * The extra headers which need to be attached to the outbound HTTP request
    */
  var headers : Map[String,Any] = Map.empty[String,Any]

  /**
    * The HTTP client
    */
  val httpClient : HttpClient = HttpClients.createDefault()

  val objectMapper : ObjectMapper = new ObjectMapper()

  /**
    * The URL the outbound request should be sent to
    */
  var url : String = null

  /**
    * A list of request headers that will not make it to the serialized version
    */
  var discardRequestHeaders : List[String] = null

  /**
    * A list of response headers that will not make it to the serialized version
    */
  var discardResponseHeaders : List[String] = null

  override def receive: Receive = {
    case msg: WebParsedResponseMessage =>
      loadConfig(getPhase(msg))
      val exportableObject = AfthemResponseSerializer.toExportableObject(msg,discardRequestHeaders, discardResponseHeaders)
      if(bufferSize > -1)
        buffer+=exportableObject
      if(bufferSize == -1) {
        log.debug("Buffer size is -1, forwarding single document")
        performRequest(Parsers.serializeAsJsonString(exportableObject, pretty = false))
      } else {
        if(buffer.size >= bufferSize) {
          log.debug("Max buffer size reached, forwarding "+buffer.size+" documents")
          performRequest(Parsers.serializeAsJsonString(buffer.toArray, pretty = false))
          buffer = new ListBuffer[Map[String,Any]]
        }
      }
  }

  /**
    * Loads the configuration from the phase
    * @param phase the phase
    */
  def loadConfig(phase : Phase) = {
    bufferSize = phase.getConfigInt("buffer_size",-1)
    headers = phase.getConfigMap("headers")
    url = phase.getConfigString("url",null)
    discardRequestHeaders = phase.getConfigList("discard_request_headers")
    discardResponseHeaders = phase.getConfigList("discard_response_headers")
  }


  /**
    * Performs the outbound HTTP request
    * @param body the serialized conversation to be forwarded
    */
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
