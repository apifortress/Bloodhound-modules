package com.apifortress.afthem.modules.swaggervalidator

import com.apifortress.afthem.messages.beans.{Header, HttpWrapper}
import com.atlassian.oai.validator.OpenApiInteractionValidator
import org.apache.commons.io.IOUtils
import org.junit.Test
import org.junit.Assert._

class SwaggerUtilTests {

  @Test
  def testComposeRequest(): Unit = {
    val wrapper = new HttpWrapper("http://example.com/foobar?a=b",-1,"POST",
                                    List(new Header("accept","application/json")),
                                  "{\"foo\":\"bar\"}".getBytes, "127.0.0.1","UTF-8")
    val request = SwaggerUtil.composeRequest(wrapper,null,false)
    assertEquals("{\"foo\":\"bar\"}",request.getBody.get())
    assertEquals("b",request.getQueryParameterValues("a").iterator().next())
    val stream = getClass.getResourceAsStream("/basic_swagger.yml")
    val data = IOUtils.toString(stream,"UTF-8")
    val validator = OpenApiInteractionValidator.createForInlineApiSpecification(data).build()
    assertFalse(validator.validateRequest(request).hasErrors)
  }
}
