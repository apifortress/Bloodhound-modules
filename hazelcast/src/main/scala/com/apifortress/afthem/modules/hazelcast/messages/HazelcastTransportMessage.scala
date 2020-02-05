package com.apifortress.afthem.modules.hazelcast.messages

import com.apifortress.afthem.messages.beans.HttpWrapper

case class HazelcastTransportMessage(val id : String, val wrapper : HttpWrapper)