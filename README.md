# Relt

Primitives for Reliable Transport. At this point, the "reliable transport" is the AMQP protocol implemented
by the RabbitMQ and that's it.

Since this will be used primarily on the [Generic Atomic Multicast](https://github.com/jabolina/go-mcast), to create
a simple structure, this Relt project will only by a RabbitMQ client with a high level API for sending messages to
fan-out exchanges.

When the [Generic Atomic Multicast](https://github.com/jabolina/go-mcast) contains the basic structure, this reliable
transport will turn into a new whole project where will be implemented a generic atomic broadcast, to be used as a
communication primitive.
