# Disable Hints

```scala
    Batch("Disable Hints", Once,
      new ResolveHints.DisableHints)
```

## ResolveHints.DisableHints

```scala
  /**
   * Removes all the hints when `spark.sql.optimizer.disableHints` is set.
   * This is executed at the very beginning of the Analyzer to disable
   * the hint functionality.
   */
  class DisableHints extends RemoveAllHints {
    override def apply(plan: LogicalPlan): LogicalPlan = {
      if (conf.getConf(SQLConf.DISABLE_HINTS)) super.apply(plan) else plan
    }
  }

  /**
   * Removes all the hints, used to remove invalid hints provided by the user.
   * This must be executed after all the other hint rules are executed.
   */
  class RemoveAllHints extends Rule[LogicalPlan] {

    private def hintErrorHandler = conf.hintErrorHandler

    def apply(plan: LogicalPlan): LogicalPlan = plan.resolveOperatorsUpWithPruning(
      _.containsPattern(UNRESOLVED_HINT)) {
      case h: UnresolvedHint =>
        hintErrorHandler.hintNotRecognized(h.name, h.parameters)
        h.child
    }
  }
```

当 `spark.sql.optimizer.disableHints`  为 `true` 时，analyzer 会后序遍历 LogicalPlan。如果节点类型为 `UnresolvedHint`, 使用其子节点代替。

Q1: UnresolvedHint 是 LogicalPlan 节点，LogicalPlan 是树结构。`h.child` 为什么可以处理度大于 1 的情况？  
A1: 这是因为 UnresolvedHint 会是 LogicalPlan 根节点，且度不会大于 1。具体可以看下面的例子  
```scala
scala> spark.sql("SELECT /*+ COALESCE(3), REBALANCE */ * from default.test;")
23/08/16 14:18:21 WARN HiveConf: HiveConf of name hive.stats.jdbc.timeout does not exist
23/08/16 14:18:21 WARN HiveConf: HiveConf of name hive.stats.retries.wait does not exist
23/08/16 14:18:24 WARN ObjectStore: Version information not found in metastore. hive.metastore.schema.verification is not enabled so recording the schema version 2.3.0
23/08/16 14:18:24 WARN ObjectStore: setMetaStoreSchemaVersion called but recording version is disabled: version = 2.3.0, comment = Set by MetaStore hengzhen.sq@127.0.0.1
23/08/16 14:18:24 WARN ObjectStore: Failed to get database global_temp, returning NoSuchObjectException
res2: org.apache.spark.sql.DataFrame = [id: int, name: string]

scala> res2.explain(extended=true)
== Parsed Logical Plan ==
'UnresolvedHint COALESCE, [3]
+- 'UnresolvedHint REBALANCE
   +- 'Project [*]
      +- 'UnresolvedRelation [default, test], [], false

== Analyzed Logical Plan ==
id: int, name: string
Repartition 3, false
+- RebalancePartitions
   +- Project [id#0, name#1]
      +- SubqueryAlias spark_catalog.default.test
         +- HiveTableRelation [`default`.`test`, org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe, Data Cols: [id#0, name#1], Partition Cols: []]

== Optimized Logical Plan ==
Repartition 3, false
+- RebalancePartitions
   +- HiveTableRelation [`default`.`test`, org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe, Data Cols: [id#0, name#1], Partition Cols: []]

== Physical Plan ==
AdaptiveSparkPlan isFinalPlan=false
+- Coalesce 3
   +- Exchange RoundRobinPartitioning(200), REBALANCE_PARTITIONS_BY_NONE, [id=#9]
      +- Scan hive default.test [id#0, name#1], HiveTableRelation [`default`.`test`, org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe, Data Cols: [id#0, name#1], Partition Cols: []]

// conf spark.sql.optimizer.disableHints: true

scala> res0.explain(extended=true)
== Parsed Logical Plan ==
'UnresolvedHint COALESCE, [3]
+- 'UnresolvedHint REBALANCE
   +- 'Project [*]
      +- 'UnresolvedRelation [default, test], [], false

== Analyzed Logical Plan ==
id: int, name: string
Project [id#0, name#1]
+- SubqueryAlias spark_catalog.default.test
   +- HiveTableRelation [`default`.`test`, org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe, Data Cols: [id#0, name#1], Partition Cols: []]

== Optimized Logical Plan ==
HiveTableRelation [`default`.`test`, org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe, Data Cols: [id#0, name#1], Partition Cols: []]

== Physical Plan ==
Scan hive default.test [id#0, name#1], HiveTableRelation [`default`.`test`, org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe, Data Cols: [id#0, name#1], Partition Cols: []]
```  