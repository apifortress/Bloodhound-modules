/**
  * Copyright 2020 API Fortress
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
package com.apifortress.afthem.modules.jdbc.actors.proxy

import java.sql.{Connection, DriverManager, ResultSet}
import java.util.Properties

import com.apifortress.afthem.{Metric, Parsers, ReqResUtil}
import com.apifortress.afthem.actors.AbstractAfthemActor
import com.apifortress.afthem.exceptions.AfthemFlowException
import com.apifortress.afthem.messages.beans.{Header, HttpWrapper}
import com.apifortress.afthem.messages.{WebParsedRequestMessage, WebParsedResponseMessage}

import scala.collection.mutable

/**
  * Companion object for UpstreamJdbcActor
  */
object UpstreamJdbcActor {

  /**
    * Determines whether the query is a 'select' or not
    * @param query the query
    * @return true if the query is a 'select'
    */
  def isSelect(query : String) : Boolean = query.toLowerCase.trim.startsWith("select")

  /**
    * Iterates over a Result Set and creates a list of maps representing the whole result set
    * @param resultSet a result set
    * @return a list of maps representing the whole result set
    */
  def resultSetToArray(resultSet : ResultSet) : List[Map[String,Any]] = {
    val columnCount = resultSet.getMetaData.getColumnCount
    val theList = mutable.MutableList[Map[String,Any]]()
    while(resultSet.next()){
      val map = mutable.HashMap[String,Any]()
      for(i <- 1 until columnCount+1){
        val name = resultSet.getMetaData.getColumnName(i)
        val value = resultSet.getObject(i)
        map.put(name,value)
      }
      theList += map.toMap
    }
    theList.toList
  }

}

/**
  * Upstream connecting to a JDBC database to work as a backend
  * @param phaseId the phase ID
  */
class UpstreamJdbcActor(phaseId : String) extends AbstractAfthemActor(phaseId: String) {


  /**
    * The JDBC connection
    */
  private var conn : Connection = _

  /**
    * The maximum number of returned rows
    */
  private var maxRows : Int = 100

  override def receive: Receive = {
    case msg : WebParsedRequestMessage =>
      loadConfig(msg)
      try {
        val m = new Metric()
        val statement = conn.createStatement()
        statement.setMaxRows(maxRows)
        statement.closeOnCompletion()
        val query = msg.request.getPayloadAsText()
        val wrapper: HttpWrapper = if (UpstreamJdbcActor.isSelect(query)) {
          val data = UpstreamJdbcActor.resultSetToArray(statement.executeQuery(query))
          new HttpWrapper(msg.request.getURL(), 200, "POST",
            List(new Header(ReqResUtil.HEADER_CONTENT_TYPE, ReqResUtil.MIME_JSON)),
            Parsers.serializeAsJsonByteArray(data, pretty = true),
            null, ReqResUtil.CHARSET_UTF8)
        } else {
          statement.execute(query)
          new HttpWrapper(msg.request.getURL(), 200, "POST",
            List(new Header(ReqResUtil.HEADER_CONTENT_TYPE, ReqResUtil.MIME_JSON)),
            "{\"status\":\"ok\"}".getBytes(ReqResUtil.CHARSET_UTF8),
            null, ReqResUtil.CHARSET_UTF8)
        }
        val message = new WebParsedResponseMessage(wrapper, msg.request, msg.backend, msg.flow, msg.deferredResult,
                                                    msg.date, msg.meta)
        forward(message)
        metricsLog.debug(m.toString())
      }catch {
        case e : Throwable => throw new AfthemFlowException(msg,e.getMessage)
      }
  }


  /**
    * Loads the config and initializes the JDBC connection
    * @param msg a WebParsedRequestMessage
    */
  def loadConfig(msg : WebParsedRequestMessage): Unit = {
    if (conn == null) {
      val phase = getPhase(msg)
      Class.forName(phase.getConfigString("driver"))
      val cProps = phase.getConfigMap("properties")
      conn = if (cProps != null) {
              val properties = new Properties()
              cProps.foreach(it => properties.put(it._1, it._2.toString))
              DriverManager.getConnection(phase.getConfigString("url"), properties)
            } else
                DriverManager.getConnection(phase.getConfigString("url"))

      maxRows = phase.getConfigInt("max_rows",100)
    }
  }

  override def postStop(): Unit = {
    super.postStop()
    conn.close()
  }
}
