# Resolve Hints

```scala
    Batch("Hints", fixedPoint,
      ResolveHints.ResolveJoinStrategyHints,
      ResolveHints.ResolveCoalesceHints),
```

## ResolveHints.ResolveJoinStrategyHints
```scala
    def apply(plan: LogicalPlan): LogicalPlan = plan.resolveOperatorsUpWithPruning(
      _.containsPattern(UNRESOLVED_HINT), ruleId) {
      case h: UnresolvedHint if STRATEGY_HINT_NAMES.contains(h.name.toUpperCase(Locale.ROOT)) =>
        if (h.parameters.isEmpty) {
          // If there is no table alias specified, apply the hint on the entire subtree.
          ResolvedHint(h.child, createHintInfo(h.name))
        } else {
          // Otherwise, find within the subtree query plans to apply the hint.
          val relationNamesInHint = h.parameters.map {
            case tableName: String => UnresolvedAttribute.parseAttributeName(tableName)
            case tableId: UnresolvedAttribute => tableId.nameParts
            case unsupported =>
              throw QueryCompilationErrors.joinStrategyHintParameterNotSupportedError(unsupported)
          }.toSet
          val relationsInHintWithMatch = new mutable.HashSet[Seq[String]]
          val applied = applyJoinStrategyHint(
            h.child, relationNamesInHint, relationsInHintWithMatch, h.name)

          // Filters unmatched relation identifiers in the hint
          val unmatchedIdents = relationNamesInHint -- relationsInHintWithMatch
          hintErrorHandler.hintRelationsNotFound(h.name, h.parameters, unmatchedIdents)
          applied
        }
    }
```  
`ResolveJoinStrategyHints` 会后序遍历 LogicalPlan，将其中 `UnresolvedHint` 替换为 `ResolvedHint`, 供后序 optimizer 识别使用。  

这里有意思的一个点在于 [SPARK-25121](https://github.com/apache/spark/pull/27935) 实现，核心函数如下:  
```scala
    private def applyJoinStrategyHint(
        plan: LogicalPlan,
        relationsInHint: Set[Seq[String]],
        relationsInHintWithMatch: mutable.HashSet[Seq[String]],
        hintName: String): LogicalPlan = {
      // Whether to continue recursing down the tree
      var recurse = true

      def matchedIdentifierInHint(identInQuery: Seq[String]): Boolean = {
        relationsInHint.find(matchedIdentifier(_, identInQuery))
          .map(relationsInHintWithMatch.add).nonEmpty
      }

      val newNode = CurrentOrigin.withOrigin(plan.origin) {
        plan match {
          case ResolvedHint(u @ UnresolvedRelation(ident, _, _), hint)
              if matchedIdentifierInHint(ident) =>
            ResolvedHint(u, createHintInfo(hintName).merge(hint, hintErrorHandler))

          case ResolvedHint(r: SubqueryAlias, hint)
              if matchedIdentifierInHint(extractIdentifier(r)) =>
            ResolvedHint(r, createHintInfo(hintName).merge(hint, hintErrorHandler))

          case UnresolvedRelation(ident, _, _) if matchedIdentifierInHint(ident) =>
            ResolvedHint(plan, createHintInfo(hintName))

          case r: SubqueryAlias if matchedIdentifierInHint(extractIdentifier(r)) =>
            ResolvedHint(plan, createHintInfo(hintName))

          case _: ResolvedHint | _: View | _: UnresolvedWith | _: SubqueryAlias =>
            // Don't traverse down these nodes.
            // For an existing strategy hint, there is no chance for a match from this point down.
            // The rest (view, with, subquery) indicates different scopes that we shouldn't traverse
            // down. Note that technically when this rule is executed, we haven't completed view
            // resolution yet and as a result the view part should be deadcode. I'm leaving it here
            // to be more future proof in case we change the view we do view resolution.
            recurse = false
            plan

          case _ =>
            plan
        }
      }

      if ((plan fastEquals newNode) && recurse) {
        newNode.mapChildren { child =>
          applyJoinStrategyHint(child, relationsInHint, relationsInHintWithMatch, hintName)
        }
      } else {
        newNode
      }
    }
```
`ResolveJoinStrategyHints` 会对按 hint 中 identifiers 顺序依次对所有的 table identifiers 进行匹配，如果满足条件就 resolve hint。这里的条件是，如果 hint 中的 identifier 位于 relation 中 identifier 队列尾部（例如 hint 中标记表 a，sql 中写的是 namespace1.namespace2.a）。规则很简单，主要是因为 `ResolveJoinStrategyHints` 运行时还未解析 unresolve relation，无法识别不同 identifier，使得匹配过程仅为文本匹配。
```
# before
'UnresolvedHint MAPJOIN, [table, table2]
+- 'Join Inner
   :- 'UnresolvedRelation [TaBlE], [], false
   +- 'UnresolvedRelation [TaBlE2], [], false

# after
'Join Inner
:- 'ResolvedHint (strategy=broadcast)
:  +- 'UnresolvedRelation [TaBlE], [], false
+- 'ResolvedHint (strategy=broadcast)
   +- 'UnresolvedRelation [TaBlE2], [], false
```
如果 Spark SQL 中设置多个 join hint，仅保留最后一个，代码用 merge 实现
```scala
          case ResolvedHint(u @ UnresolvedRelation(ident, _, _), hint)
              if matchedIdentifierInHint(ident) =>
            ResolvedHint(u, createHintInfo(hintName).merge(hint, hintErrorHandler))

          case ResolvedHint(r: SubqueryAlias, hint)
              if matchedIdentifierInHint(extractIdentifier(r)) =>
            ResolvedHint(r, createHintInfo(hintName).merge(hint, hintErrorHandler))
```


## ResolveHints.ResolveCoalesceHints

该 Rule 处理 `REPARTITION` 、`COALESCE`、`REPARTITION_BY_RANGE` 和 `REBALANCE` 四个 hint。需要简单说明一下前两者之间的区别：  
* repartition: 用于增加或减少 partition 数量，会引入 shuffle
* coalesce: 用于减少 partition 数量，不会触发 shuffle

```scala
    def apply(plan: LogicalPlan): LogicalPlan = plan.resolveOperatorsWithPruning(
      _.containsPattern(UNRESOLVED_HINT), ruleId) {
      case hint @ UnresolvedHint(hintName, _, _) => hintName.toUpperCase(Locale.ROOT) match {
          case "REPARTITION" =>
            createRepartition(shuffle = true, transformStringToAttribute(hint))
          case "COALESCE" =>
            createRepartition(shuffle = false, transformStringToAttribute(hint))
          case "REPARTITION_BY_RANGE" =>
            createRepartitionByRange(transformStringToAttribute(hint))
          case "REBALANCE" if conf.adaptiveExecutionEnabled =>
            createRebalance(transformStringToAttribute(hint))
          case _ => hint
        }
    }
```

### REPARTITION & COALESCE
```scala
    /**
     * This function handles hints for "COALESCE" and "REPARTITION".
     * The "COALESCE" hint only has a partition number as a parameter. The "REPARTITION" hint
     * has a partition number, columns, or both of them as parameters.
     */
    private def createRepartition(
        shuffle: Boolean, hint: UnresolvedHint): LogicalPlan = {
      val hintName = hint.name.toUpperCase(Locale.ROOT)

      def createRepartitionByExpression(
          numPartitions: Option[Int], partitionExprs: Seq[Any]): RepartitionByExpression = {
        val sortOrders = partitionExprs.filter(_.isInstanceOf[SortOrder])
        if (sortOrders.nonEmpty) {
          throw QueryCompilationErrors.invalidRepartitionExpressionsError(sortOrders)
        }
        val invalidParams = partitionExprs.filter(!_.isInstanceOf[UnresolvedAttribute])
        if (invalidParams.nonEmpty) {
          throw QueryCompilationErrors.invalidHintParameterError(hintName, invalidParams)
        }
        RepartitionByExpression(
          partitionExprs.map(_.asInstanceOf[Expression]), hint.child, numPartitions)
      }

      hint.parameters match {
        case Seq(IntegerLiteral(numPartitions)) =>
          Repartition(numPartitions, shuffle, hint.child)
        // The "COALESCE" hint (shuffle = false) must have a partition number only
        case _ if !shuffle =>
          throw QueryCompilationErrors.invalidCoalesceHintParameterError(hintName)

        case param @ Seq(IntegerLiteral(numPartitions), _*) if shuffle =>
          createRepartitionByExpression(Some(numPartitions), param.tail)
        case param @ Seq(_*) if shuffle =>
          createRepartitionByExpression(None, param)
      }
    }
```
如果参数只有数字，就创建一个 Repartition 。如果包含 partition，进行 sortOrder、unresolvedAttribute 检验后，创建 RepartitionByExpression。

```scala
// Dataframe with one column "value" containing 1000000 the number 0 in addition to the numbers 5000, 10000 and 100000
> val df = Seq((0 to 1000000).map(_ => 0) :+ 5000 :+ 10000 :+ 100000: _*).toDF("value")

> df.distinct.show

+------+                                                                        
| value|
+------+
|     0|
|100000|
|  5000|
| 10000|
+------+

> df.repartition(4, col("value")).withColumn("partition", spark_partition_id()).groupBy(col("partition")).agg(count(col("value")).as("count"),min(col("value")).as("min_value"),max(col("value")).as("max_value")).orderBy(col("partition")).show

+---------+-------+---------+---------+
|partition|  count|min_value|max_value|
+---------+-------+---------+---------+
|        0|      1|   100000|   100000|
|        1|      1|    10000|    10000|
|        2|      1|     5000|     5000|
|        3|1000001|        0|        0|
+---------+-------+---------+---------+

> df.repartition(4).withColumn("partition", spark_partition_id()).groupBy(col("partition")).agg(count(col("value")).as("count"),min(col("value")).as("min_value"),max(col("value")).as("max_value")).orderBy(col("partition")).show

+---------+------+---------+---------+                                          
|partition| count|min_value|max_value|
+---------+------+---------+---------+
|        0|250004|        0|   100000|
|        1|249996|        0|        0|
|        2|249996|        0|     5000|
|        3|250008|        0|    10000|
+---------+------+---------+---------+
```
Repartition 参数提供一列或多列时，重分区采用 HashPartitioner，值将被散列并用于通过计算诸如`partition = hash(columns) % numberOfPartitions` 之类的内容来确定分区号。参数没有提供列时，重新分区应用 RoundRobinPartitioner，数据将均匀分布在指定数量的分区上。

ps. 这里是用 dataframe 来展示 repartition hint 效果。

### REPARTITION_BY_RANGE

```scala
    /**
     * This function handles hints for "REPARTITION_BY_RANGE".
     * The "REPARTITION_BY_RANGE" hint must have column names and a partition number is optional.
     */
    private def createRepartitionByRange(hint: UnresolvedHint): RepartitionByExpression = {
      val hintName = hint.name.toUpperCase(Locale.ROOT)

      def createRepartitionByExpression(
          numPartitions: Option[Int], partitionExprs: Seq[Any]): RepartitionByExpression = {
        val invalidParams = partitionExprs.filter(!_.isInstanceOf[UnresolvedAttribute])
        if (invalidParams.nonEmpty) {
          throw QueryCompilationErrors.invalidHintParameterError(hintName, invalidParams)
        }
        val sortOrder = partitionExprs.map {
          case expr: SortOrder => expr
          case expr: Expression => SortOrder(expr, Ascending)
        }
        RepartitionByExpression(sortOrder, hint.child, numPartitions)
      }

      hint.parameters match {
        case param @ Seq(IntegerLiteral(numPartitions), _*) =>
          createRepartitionByExpression(Some(numPartitions), param.tail)
        case param @ Seq(_*) =>
          createRepartitionByExpression(None, param)
      }
    }
```
核心就是加上 SortOrder 创建 RepartitionByExpression。

```scala
// Dataframe with one column "value" containing the values ranging from 0 to 1000000
> val df = Seq(0 to 1000000: _*).toDF("value")

// repartition
> df.repartition(4, col("value")).withColumn("partition", spark_partition_id()).groupBy(col("partition")).agg(count(col("value")).as("count"),min(col("value")).as("min_value"),max(col("value")).as("max_value")).orderBy(col("partition")).show

+---------+------+---------+---------+
|partition|count |min_value|max_value|
+---------+------+---------+---------+
|0        |249911|12       |1000000  |
|1        |250076|6        |999994   |
|2        |250334|2        |999999   |
|3        |249680|0        |999998   |
+---------+------+---------+---------+

//repartitionByRange
> df.repartitionByRange(4, col("value")).withColumn("partition", spark_partition_id()).groupBy(col("partition")).agg(count(col("value")).as("count"),min(col("value")).as("min_value"),max(col("value")).as("max_value")).orderBy(col("partition")).show

+---------+------+---------+---------+
|partition|count |min_value|max_value|
+---------+------+---------+---------+
|0        |244803|0        |244802   |
|1        |255376|244803   |500178   |
|2        |249777|500179   |749955   |
|3        |250045|749956   |1000000  |
+---------+------+---------+---------+
```

repartitionByRange 将根据列值的范围对数据进行分区。 这通常用于连续（非离散）值，例如任何类型的数字。由于性能原因，此方法使用采样来估计范围。 因此，输出可能不一致，因为采样可能返回不同的值。