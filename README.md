# AFtheM Modules

AFtheM is a modular open source microgateway that simplifies the creation of complex flows. Visit the [AFtheM GitHub repository](https://github.com/apifortress/afthem) for more info.

In addition to the default components already present in the software, other official pluggable modules are available in this repository.

All these modules are packaged in the AFtheM Docker images that can be found on [Docker Hub](https://hub.docker.com/r/apifortress/afthem).

## Fortress Forwarder

Actors useful to capture and forward API conversations to HTTP endpoints.

## JDBC

Use relational databases as upstreams and make them available using API calls.

## MongoDB

This module serves four purposes purposes:

* Use MongoDB as upstream and make it available using API calls

* Store and use AFtheM configuration files in MongoDB

* Store access logs in MongoDB

* Capture and store API conversations in MongoDB

## RabbitMQ

Capiture API conversations and forward them to a RabbitMQ exchange.
