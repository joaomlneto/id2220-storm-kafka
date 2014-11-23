id2220-storm-kafka
KTH Implementation of Distributed Systems Course Project
==================

This project aims to measure delays in Apache Storm with various different guarantees. 

We are mainly investigaing processing delay of Apache Storm in real datasets. Our initial test case is degree count topology running on top Apache Storm with no processing guarantee, at least once processing guarantee and exactly-once processing guarantee. To achieve exactly-once processing guarantee, we will use an abstraction API called Trident.

Our second goal is to investigate the possibilities of implementing ideas of MillWheel framework for Apache Storm. However, this goal is currently on hold.

Currenty we use, Apache Storm v0.9.2-incubating for our experiments.

We use kafka to produce tuples into our system with the help of KafkaSpout. 


