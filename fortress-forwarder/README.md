# AFtheM - Fortress Forwarder module

Serializes the API conversation using the default AFtheM implementation and forwards the result via HTTP. While the
module has been originally designed to work with the API Fortress suite, it can easily be used with any other system.

## Sidecars

### FortressForwarderSerializerActor

**class:** `com.apifortress.afthem.modules.fortressforwarders.actors.sidecars.serializers.FortressForwarderSerializerActor`

**sidecars:** no

**configuration:**

General serializer settings:

* `disable_on_header`: if the provided header is present in the request, then the conversation will skip serialization
* `enable_on_header`: if the provided header is present in the request, then the conversation will be serialized
* `discard_request_headers`: list of request headers that should not appear in the serialized conversation
* `discard_response_headers`: list of response headers that should not appear in the serialized conversation
* `allow_content_types`: full or partial response content types which make the request eligible for serialization. If
the list is null or empty, all content types will be accepted

Extra serializer settings:

* `buffer_size`: the serializer can buffer a number of conversations and save them once the buffer is full to improve
DB communication performance. If absent or if the value is less than 1, the document is inserted as asson as the actor
receives it

Forwarder settings:

* `url`: the URL to POST the serialized conversation to
* `headers`: a key/value map of request headers to be added to the outbound request 