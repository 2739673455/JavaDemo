package com.atguigu.flink.architecture;

/*
slot共享
     Flink允许作业中上下游算的的并行实例共享同一个slot

为什么要共享:
     1. 一个slot可以持有整个作业管道
     2. Flink集群所需的taskslot和作业中使用的最大并行度恰好一样，无需计算程序总共包含多少个task
     3. 更好的资源利用。如果没有 slot 共享，非密集 subtask（source/map()）将阻塞和密集型 subtask（window） 一样多的资源。
      同时确保繁重的 subtask 在 TaskManager 之间公平分配

可通过设置共享组来不共享slot
     slotSharingGroup("group1")
从source算子开始，默认将算子放到默认的default共享组；后续的算子如果不明确指定共享组，默认从上游算子继承共享组
 * */

public class Flink03_TaskSlot {
}
