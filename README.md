# Relt

Primitives for Reliable Transport. The communication will be backed by RabbitMQ using the
[quorum-queues](https://www.rabbitmq.com/quorum-queues.html) that uses the Raft protocol,
thus we have total order for messages deliveries. For handling the messages before publishing into
the RabbitMQ exists a distributed queue also backed by the Raft protocol, using [Hashicorp](https://github.com/hashicorp/raft)
implementation.

 This project will create primitives for a reliable broadcast across multiple peers. Each 
 group of peers will listen to the same exchange. Each instance of the relt struct will
 be replicated on *N* publishers, responsible to publish the message on the RabbitMQ exchange
 and also each publisher is a Raft member.
 
 When a new message is broadcast to a group, it will be inserted in a distributed in-memory queue
 backed by the Raft protocol. Once the message is committed on the queue, it will be published on the
 RabbitMQ exchange.
 
 After a message is successfully sent to the RabbitMQ broker, the publisher will receive an ACK and
 the message will be removed from the distributed queue and once the message is removed the next
 message will be polled from the head and consumed.
 
 The structure of the project is:
 
 <p align="center">
   <img alt="project structure" src="https://user-images.githubusercontent.com/13581903/85213786-9dca2580-b339-11ea-9bc2-d0167ec585e5.png">
 </p>
