# Relt

Primitives for Reliable Transport. The communication will be backed by RabbitMQ using the
[quorum-queues](https://www.rabbitmq.com/quorum-queues.html) that uses the Raft protocol,
thus we have total order for messages deliveries.

 This project will create primitives for a reliable broadcast across multiple peers. Each 
 group of peers will listen to the same exchange.
