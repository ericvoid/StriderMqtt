Strider MQTT
============

A very thin MQTT client written in C#. This library isn't a full fledged MQTT
client, but implements a `MqttConnection` that encapsulates lower level aspects
of the protocol (like sockets, packet reading and writing, and so on).

The library was written using MonoDevelop, but it doesn't require too many
changes to work with VisualStudio.

Features
--------

The current version has the following features:

* MQTT 3.1 and 3.1.1 versions;
* QoS level 0, 1 and 2;
* TCP and TLS connections;

The following also should be acknowledged.

This library does *not* use concurrency or parallelism internally, as it is
intended to use minimum resources.

This lib also does *not* persist anything. If you intend to use QoS 1 or 2
you will have to implement persistence by yourself. See the
`SqlitePersistenceSample` project, it shows how to implement it appropriately.

Documentation and Help
----------------------

Check the [wiki pages](https://github.com/ericvoid/StriderMqtt/wiki).

References
----------

The MQTT Protocol [official website](http://mqtt.org).

Overall architecture inspired by [Mosquitto's Python client](http://mosquitto.org/documentation/python/).

Packets reading and writing inspired by
[MqttDotNet](https://github.com/stevenlovegrove/MqttDotNet).
