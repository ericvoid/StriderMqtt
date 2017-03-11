Strider MQTT
============

A very thin MQTT client written in C#. This library isn't a full fledged MQTT
client, but made with simplicity (the first MQTT principle) in mind. It
basically implements a `MqttConnection` that encapsulates lower level aspects
of the protocol (like sockets, packet reading and writing, and so on).

The library was written using MonoDevelop, but it shouldn't require too many
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

This lib provides classes for persistency. By default, it uses memory only. So
if you intend to use QoS 1 or 2 in an unstable environment, prone to hardware,
os or process failure, you should persist in disk. A Sqlite implementation is
provided, but you can implement `IMqttPersistence` to suit your needs.

This lib does not guarantee ordering if more than one message is inflight
per channel (incoming / outgoing).

Documentation and Help
----------------------

To publish a QoS 0 "hello world" message, all you need to do is the following:

```C#
    var connArgs = new MqttConnectionArgs()
    {
        ClientId = "my-strider-client",
        Hostname = "some-broker.com",
        Port = 1883
    };

    using (var conn = new MqttConnection(connArgs))
    {
        conn.Connect();

        conn.Publish(new PublishPacket() {
            QosLevel = MqttQos.AtMostOnce,
            Topic = "my/test/topic",
            Message = Encoding.UTF8.GetBytes("Hello world!")
        });

        conn.Disconnect();
    }
```

If you need more, check the [wiki pages](https://github.com/ericvoid/StriderMqtt/wiki).

References
----------

The MQTT Protocol [official website](http://mqtt.org).

Overall architecture inspired by [Mosquitto's Python client](http://mosquitto.org/documentation/python/).

Packets reading and writing inspired by
[MqttDotNet](https://github.com/stevenlovegrove/MqttDotNet).
