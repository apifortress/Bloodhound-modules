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
package com.apifortress.afthem.modules.swaggervalidator.actors.sidecars

import java.io.File

import com.apifortress.afthem.Metric
import com.apifortress.afthem.actors.AbstractAfthemActor
import com.apifortress.afthem.config.{AfthemCache, Phase}
import com.apifortress.afthem.messages.{WebParsedRequestMessage, WebParsedResponseMessage}
import com.apifortress.afthem.modules.swaggervalidator.SwaggerUtil
import com.atlassian.oai.validator.OpenApiInteractionValidator
import org.slf4j.LoggerFactory

/**
  * A checker sidecar that validates requests/responses against a Swagger file, and outputs
  * failures to comply in a log file
  * @param phaseId the phase ID
  */
class SwaggerCheckerActor(phaseId: String) extends AbstractAfthemActor(phaseId) {

  /**
    * True if the URL match should happen on the upstream URL and not on the request URL.
    * This is effective only when the filter is placed before the upstream operation
    */
  var matchUpstream : Boolean = false
  /**
    * The validator
    */
  var validator : OpenApiInteractionValidator = _

  /**
    * The inbound logger
    */
  private val swaggerValidatorLogger = LoggerFactory.getLogger("Swagger-Validator")

  override def receive: Receive = {
    case msg : WebParsedRequestMessage =>
      val m = new Metric()
      loadConfig(getPhase(msg))
      val validationReport = validator.validateRequest(SwaggerUtil.composeRequest(msg.request,msg.backend,matchUpstream))
      if(validationReport.hasErrors)
        swaggerValidatorLogger.info("Invalid - "+msg.request.remoteIP + " - " + msg.request.method + " " + msg.request.getURL() + " - ERROR: "+validationReport.getMessages().toString)
    case msg : WebParsedResponseMessage =>
      val m = new Metric()
      loadConfig(getPhase(msg))
      val validationReport = validator.validate(SwaggerUtil.composeRequest(msg.request,msg.backend,false),
        SwaggerUtil.composeResponse(msg))
      if(validationReport.hasErrors)
        swaggerValidatorLogger.info("Invalid - "+msg.request.remoteIP + " - " + msg.request.method + " " + msg.request.getURL() + " - ERROR: "+validationReport.getMessages().toString)
      metricsLog.debug(m.toString())
  }

  /**
    * Loads the configuration
    * @param phase the phase
    */
  private def loadConfig(phase : Phase) = {
    matchUpstream = phase.getConfigBoolean("match_upstream").getOrElse(false)
    val file = new File(phase.getConfigString("filename"))
    val signature = phase.getConfigString("filename")+"_"+file.lastModified().toString
    validator = AfthemCache.configExtraCache.get(signature).asInstanceOf[OpenApiInteractionValidator]
    if(validator == null) {
      validator = OpenApiInteractionValidator.createFor(file.getAbsolutePath).build()
      AfthemCache.configExtraCache.put(signature,validator)
    }
  }
}
