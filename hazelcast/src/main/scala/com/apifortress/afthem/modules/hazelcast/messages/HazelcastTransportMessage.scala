/*
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
package com.apifortress.afthem.modules.hazelcast.messages

import com.apifortress.afthem.messages.beans.HttpWrapper

/**
  * Transport message for Hazelcast connections
  * @param id the ID of the message
  * @param wrapper either a request or response wrapper
  */
case class HazelcastTransportMessage(id : String, wrapper : HttpWrapper)