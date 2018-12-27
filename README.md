# RabbitMQ Deep Match Exchange Type

## Original

This plugin is a fork of [rabbitmq/rabbitmq-rtopic-exchange](https://github.com/rabbitmq/rabbitmq-rtopic-exchange).

## Description

This plugin creates a new exchange type called `x-deepmatch` which, routes exchange messages according to a longest shared prefix. The intent of this plugin is to enable the usage of a "most-specific-first" routing algorithm. For instance, if one has a queue that knows how to handle `entity.vehicle` and another that knows how to handle `entity.vehicle.car`, if we insert something into the queue with routing key `entity.vehicle.car.honda`, it should be routed to the latter queue.

### Examples

Say that we have three queues bound to our exchange

 - `A` has routing key `entity.vehicle`
 - `B` has routing key `entity.vehicle.car`
 - `C` has routing key `entity.vehicle.plane`

Then here is how different messages will be routed

 - `entity.vehicle` --> `A`
 - `entity.vehicle.car` --> `B`
 - `entity.vehicle.boat` --> `A`
 - `entity.vehicle.plane.boeing` --> `C`
 - `entity.*.car` --> `B`
 - `entity.vehicle.*` --> **Arbitrarily** between `B` and `C`
 - `entity` --> Dropped
 - ` ` --> Dropped

## Future Work

Note that currently no steps are taken to restore messages in a queue that is lost or unbound. Future work may be taken to reinsert all messages that would be lost due to this. 

## Authors

The original work is (c) Pivotal Software Inc., 2007-2016 and originally developed by Alvaro Videla.

This fork is written and maintained by @zwade
