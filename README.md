# Relt

![Go](https://github.com/jabolina/relt/workflows/Go/badge.svg)

A primitive for reliable communication, backed by the atomic broadcast protocol. An example can be found on the
`_examples` folder, the current atomic broadcast implementation is the [etcd](https://github.com/etcd-io/etcd)
version, which means that an etcd server is needed.

Since this will be used primarily on the [Generic Atomic Multicast](https://github.com/jabolina/go-mcast), to create
a simple structure, with a high level API for sending messages to a group of processes.

When the [Generic Atomic Multicast](https://github.com/jabolina/go-mcast) contains the basic structure, this reliable
transport will turn into a new whole project where will be implemented a generic atomic broadcast, to be used as the 
default reliable communication primitive.
