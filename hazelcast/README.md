# AFtheM - Hazelcast Actors

The proposed actors create a persistent tunnel between two AFtheM instances.

The **server** instance waits a connection from the client instance. Its role is to accept HTTP requests and forward
them to the client.

The **client** instance initiates a connection to the server and receives forwarded requests to actually execute them
against the defined upstream.

The system has been designed to invert the connection direction to bypass inbound firewall rules in datacenters. 

## Type: Proxy

### UpstreamHazelcastActor

This upstream, by behaving as a Hazelcast server, will allow connection tunnelling between this instance of AFtheM and
a remote instance, working as a Hazelcast client.

**class:** `com.apifortress.afthem.modules.hazelcast.actors.proxy.UpstreamHazelcastActor`

**sidecars:** yes

**config:**

* `remote_id`: the ID of the remote AFtheM counterpart

**Note:** other Hazelcast settings can be placed in the `etc/hazelcast.xml` file.

## Type: Ingress

### HazelcastIngressActor

The ingress acting as an Hazelcast client.

**class:** `com.apifortress.afthem.modules.hazelcast.actors.ingresses.HazelcastIngressActor`

**sidecars:** no

**config:**

Configuration must be placed in the `implementers.yml` file.

* `name`: the ID of the AFtheM Hazelcast client
* `server`: the address of the AFtheM Hazelcast server

**Note:** other Hazelcast settings can be placed in the `etc/<name>_hazelcastClient.xml` file.