/*
 *   Copyright 2020 API Fortress
 *   Licensed under the Apache License, Version 2.0 (the "License");
 *   you may not use this file except in compliance with the License.
 *   You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 *   @author Simone Pezzano
 *
 */
package com.apifortress.afthem.modules.swaggervalidator

import com.apifortress.afthem.UriUtil
import com.apifortress.afthem.config.Backend
import com.apifortress.afthem.messages.WebParsedResponseMessage
import com.apifortress.afthem.messages.beans.HttpWrapper
import com.atlassian.oai.validator.model.{SimpleRequest, SimpleResponse}

object SwaggerUtil {

  /**
    * Composes a SimpleRequest, necessary for validation fo requests
    * @param request the request HttpWrapper
    * @param backend the backend configuration
    * @param matchUpstream set to true when the path should match the upstream path rather than the request path
    * @return the SimpleRequest
    */
  def composeRequest(request : HttpWrapper, backend: Backend, matchUpstream: Boolean): SimpleRequest = {
    val upstreamPart = if(matchUpstream)
      UriUtil.stripQueryString(UriUtil.determineUpstreamPart(request.uriComponents, backend))
    else
      request.uriComponents.getPath

    var builder = request.method match  {
      case "GET" =>
        SimpleRequest.Builder.get(upstreamPart)
      case "POST" =>
        SimpleRequest.Builder.post(upstreamPart).withBody(request.getPayloadAsText())
      case "PUT" =>
        SimpleRequest.Builder.put(upstreamPart).withBody(request.getPayloadAsText())
      case "DELETE" =>
        SimpleRequest.Builder.delete(upstreamPart).withBody(request.getPayloadAsText())
    }
    request.headers.foreach( it => {
      builder = builder.withHeader(it.key, it.value)
    })

    request.uriComponents.getQueryParams.forEach((k,v) => builder.withQueryParam(k,v))

    builder.build()
  }

  /**
    * Composes a SimpleResponse necessary to validate a response
    * @param msg a WebParsedResponseMessage
    * @return a SimpleResponse
    */
  def composeResponse(msg : WebParsedResponseMessage) : SimpleResponse = {
    var builder = SimpleResponse.Builder.status(msg.response.status)
    msg.response.headers.foreach( it =>{
      builder = builder.withHeader(it.key,it.value)
    })
    builder = builder.withBody(msg.response.getPayloadAsText())
    builder.build()
  }
}
