Strider MQTT
============

A very thin MQTT client written in C#, suitable for use with Mono and .Net
(will need a few modifications). Works with MQTT 3.1 and 3.1.1 protocol
versions. Refer to [official specs](http://mqtt.org/documentation).

This library isn't a full fledged MQTT client, but implements a `MqttConnection`
that encapsulates lower level aspects of the protocol (like sockets, packet
reading and writing, and so on).

This lib also does not use threads, so it won't eat up your precious resources.

The `MqttConnection` does not persist anything. If you intend to use QoS 1 or 2
you will have to implement persistence by yourself. 
See the `SqlitePersistenceSample` project, it shows how to implement it
appropriately.

Tests
-----

There are no unit tests, but there is an integration test. It runs using the
`NumbersTest` project's executable and the Python scripts in `testutils`
directory.

The `NumbersTest` is a client that publishes a sequence of numbers to a topic,
and subscribes to that same topic. It runs until all the sequence is received
back. Its result can be verified for duplication, loss of messages and loss of
ordering.

The `runprocesses.py` script spaws many `NumbersTest` processes. You can for
example issue:

    python runprocesses.py 7 2 4 100

This will:

* start the run number 7 (used as prefix for client-ids and topics);
* use QoS 2;
* spaw 4 processes;
* make all clients subscribe to the same topic;
* make each client publish 100 numbers (from 1 to 100 inclusive);
* make each client receive 400 numbers (4 processes * 100 numbers each).

Just to make things a little more fun, the script randomly kills one of those
process from time to time.

In the end, each of them will have to have all the sequence of numbers of all
the clients. The script itself validates the result.

References
----------

The MQTT Protocol [official website](http://mqtt.org).

Packets reading and writing inspired by
[MqttDotNet](https://github.com/stevenlovegrove/MqttDotNet).
