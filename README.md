# Easy-Scheduler

## 介绍

一款基于有向无环图(DAG)的本地任务编排和调度执行器，设计目标是轻量、简单易用。

## 特点

- 轻量：注重轻量级设计，依赖少，保持高效性能
- 简单易用：提供简单直观的API
- 低侵入性：基于注解来定义和配置DAG，使得DAG的定义更加直观和简洁。
- 支持表达式：集成 Spring Expression Language（SpEL）以提供更灵活配置

## 项目概览

### Dag核心驱动设计

### 项目模块说明

- easy-scheduler-core: 核心基础模块，定义节点
- easy-scheduler-reader：负责读取文本配置、注解配置
- easy-scheduler-spel：支持spel表达式
- easy-scheduler-dynamic：基于core、reader模块，提供动态构建Dag图的能力

### 项目组件说明

- NodeHandler：一个节点处理器
- DagNode：Dag图节点的定义，内包含对应的处理器，以及对其他节点依赖关系
- DagGraph：Dag图的抽象，是DagNode的集合，维护图的关系
- DagEngine：Dag执行引擎
- GraphDefinition、NodeDefinition：基础的Dag图、节点定义
- DagGraphReader：负责将文本配置内容、解析使用了注解的Class，将器转换成基础的GraphDefinition
- DagGraphFactory：负责将GraphDefinition转换成DagGraph

## 使用示例

### 基础示例

基础示例也是原始的底层使用方式，只需要引用用 easy-scheduler-core 模块即可。

### SpringBoot集成示例

和Springboot集成，使用了所有的模块，并且支持自动注册Handler。



