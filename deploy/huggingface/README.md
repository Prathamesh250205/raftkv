---
title: raftkv live demo
emoji: 🗳️
colorFrom: blue
colorTo: yellow
sdk: docker
app_port: 7860
pinned: true
short_description: A Raft key-value store you can crash and watch recover
---

# raftkv: live demo

A 3-node, 3-shard cluster of [raftkv](https://github.com/Prathamesh250205/raftkv), a sharded,
linearizable key-value store built on a from-scratch implementation of the Raft consensus
algorithm in Go.

Write and read keys, then **crash a node** and watch the surviving nodes elect a new leader in
about 300 ms, with no data lost. The crashed node restarts from its disk a few seconds later and
catches up.

This is a sandbox: data resets when the Space restarts, only one node can be down at a time, and
values are limited to 256 bytes. Source, design and benchmarks are on
[GitHub](https://github.com/Prathamesh250205/raftkv).
