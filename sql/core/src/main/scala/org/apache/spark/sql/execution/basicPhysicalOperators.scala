/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.sql.execution

import scala.concurrent.{ExecutionContext, Future}
import scala.concurrent.duration.Duration

import org.apache.spark.SparkException
import org.apache.spark.rdd.{PartitionwiseSampledRDD, RDD}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.codegen.{CodegenContext, ExprCode, ExpressionCanonicalizer}
import org.apache.spark.sql.catalyst.plans.physical._
import org.apache.spark.sql.execution.metric.SQLMetrics
import org.apache.spark.sql.execution.ui.SparkListenerDriverAccumUpdates
import org.apache.spark.sql.types.LongType
import org.apache.spark.util.ThreadUtils
import org.apache.spark.util.random.{BernoulliCellSampler, PoissonSampler}

/** Physical plan for Project. */
case class ProjectExec(projectList: Seq[NamedExpression], child: SparkPlan)
  extends UnaryExecNode with CodegenSupport {

  override def output: Seq[Attribute] = projectList.map(_.toAttribute)

  override def inputRDDs(): Seq[RDD[InternalRow]] = {
    child.asInstanceOf[CodegenSupport].inputRDDs()
  }

  protected override def doProduce(ctx: CodegenContext): String = {
    child.asInstanceOf[CodegenSupport].produce(ctx, this)
  }

  override def usedInputs: AttributeSet = {
    // only the attributes those are used at least twice should be evaluated before this plan,
    // otherwise we could defer the evaluation until output attribute is actually used.
    val usedExprIds = projectList.flatMap(_.collect {
      case a: Attribute => a.exprId
    })
    val usedMoreThanOnce = usedExprIds.groupBy(id => id).filter(_._2.size > 1).keySet
    references.filter(a => usedMoreThanOnce.contains(a.exprId))
  }

  override def doConsume(ctx: CodegenContext, input: Seq[ExprCode], row: ExprCode): String = {

    // 根据Project List得到输出列的表达式列表
    val exprs: Seq[Expression] = projectList.map(x =>
      ExpressionCanonicalizer.execute(BindReferences.bindReference(x, child.output)))

    // 设置为传递进来的输入变量
    ctx.currentVars = input

    // 生成Project List表达式的代码作为结果列
    val resultVars = exprs.map(_.genCode(ctx))

    // Evaluation of non-deterministic expressions can't be deferred.
    // 提取不确定列表达式
    val nonDeterministicAttrs = projectList.filterNot(_.deterministic).map(_.toAttribute)
    s"""
       |// [ProjectExec#doConsume] 针对不确定表达式的代码生成，比如rand()函数等待
       |${evaluateRequiredVariables(output, resultVars, AttributeSet(nonDeterministicAttrs))}
       |// [ProjectExec#doConsume] 调用consume，继续调用父类doConsume方法
       |${consume(ctx, resultVars)}
     """.stripMargin
  }

  protected override def doExecute(): RDD[InternalRow] = {
    child.execute().mapPartitionsWithIndexInternal { (index, iter) =>
      // 会调用GenerateUnsafeProjection对象的generate方法生成UnsafeProjection类，完成投影算子的逻辑。
      val project = UnsafeProjection.create(projectList, child.output,
        subexpressionEliminationEnabled)
      project.initialize(index)
      iter.map(project)
    }
  }

  override def outputOrdering: Seq[SortOrder] = child.outputOrdering

  override def outputPartitioning: Partitioning = child.outputPartitioning
}


/** Physical plan for Filter. */
case class FilterExec(condition: Expression, child: SparkPlan)
  extends UnaryExecNode with CodegenSupport with PredicateHelper {

  // Split out all the IsNotNulls from condition.
  /**
   * 将谓词分为notNull和其他类型，例如对于谓词age > 22，会产生IsNotNull(age)和GreaterThan(age, 22)两个谓词，
   * 前一个为notNull类型谓词，后一个为其他类型谓词。
   */
  private val (notNullPreds, otherPreds) = splitConjunctivePredicates(condition).partition {
    case IsNotNull(a) => isNullIntolerant(a) && a.references.subsetOf(child.outputSet)
    case _ => false
  }

  // If one expression and its children are null intolerant, it is null intolerant.
  private def isNullIntolerant(expr: Expression): Boolean = expr match {
    case e: NullIntolerant => e.children.forall(isNullIntolerant)
    case _ => false
  }

  // The columns that will filtered out by `IsNotNull` could be considered as not nullable.
  private val notNullAttributes = notNullPreds.flatMap(_.references).distinct.map(_.exprId)

  // Mark this as empty. We'll evaluate the input during doConsume(). We don't want to evaluate
  // all the variables at the beginning to take advantage of short circuiting.
  override def usedInputs: AttributeSet = AttributeSet.empty

  override def output: Seq[Attribute] = {
    child.output.map { a =>
      if (a.nullable && notNullAttributes.contains(a.exprId)) {
        a.withNullability(false)
      } else {
        a
      }
    }
  }

  override lazy val metrics = Map(
    "numOutputRows" -> SQLMetrics.createMetric(sparkContext, "number of output rows"))

  override def inputRDDs(): Seq[RDD[InternalRow]] = {
    child.asInstanceOf[CodegenSupport].inputRDDs()
  }

  protected override def doProduce(ctx: CodegenContext): String = {
    child.asInstanceOf[CodegenSupport].produce(ctx, this)
  }

  override def doConsume(ctx: CodegenContext, input: Seq[ExprCode], row: ExprCode): String = {
    /**
     * 添加filter_numOutputRows变量，用来记录经过过滤处理之后的数据行数。
     * - 变量名：filter_numOutputRows
     * - 类型：SQLMetrics
     * - 初始化代码：this.filter_numOutputRows = (org.apache.spark.sql.execution.metrics.SQLMetric) references[1];
     */
    val numOutput = metricTerm(ctx, "numOutputRows")

    /**
     * Generates code for `c`, using `in` for input attributes and `attrs` for nullability.
     *
     * 过滤条件代码生成。
     */
    def genPredicate(c: Expression, in: Seq[ExprCode], attrs: Seq[Attribute]): String = {
      val bound = BindReferences.bindReference(c, attrs)
      // evaluateRequiredVariables(attributes: Seq[Attribute], variables: Seq[ExprCode], required: AttributeSet)
      // 从子节点输出列中找出需要的列的代码段，按行分隔
      val evaluated = evaluateRequiredVariables(child.output, in, c.references)

      // Generate the code for the predicate.
      // 生成谓词代码
      val ev = ExpressionCanonicalizer.execute(bound).genCode(ctx)

      // Null值判断代码
      val nullCheck = if (bound.nullable) {
        s"${ev.isNull} || "
      } else {
        s""
      }

      // 拼装列声明代码、谓词判断执行代码、Null值判断代码
      s"""
         |// [FilterExec#doConsume] 列声明代码
         |$evaluated
         |// [FilterExec#doConsume] 谓词判断执行代码
         |${ev.code}
         |// [FilterExec#doConsume] if中可能存在的Null值判断条件
         |if (${nullCheck}!${ev.value}) continue;
       """.stripMargin
    }

    // CodegenContext中的currentVars会设置为当前传递进来的输入变量所对应的ExprCode对象
    ctx.currentVars = input

    // To generate the predicates we will follow this algorithm.
    // For each predicate that is not IsNotNull, we will generate them one by one loading attributes
    // as necessary. For each of both attributes, if there is an IsNotNull predicate we will
    // generate that check *before* the predicate. After all of these predicates, we will generate
    // the remaining IsNotNull checks that were not part of other predicates.
    // This has the property of not doing redundant IsNotNull checks and taking better advantage of
    // short-circuiting, not loading attributes until they are needed.
    // This is very perf sensitive.
    // TODO: revisit this. We can consider reordering predicates as well.

    /**
     * 为了生成谓词，我们将遵循这个算法。
     * 对于每个不是 IsNotNull 的谓词，我们会根据需要一一生成它们的加载属性。
     * 对于这两个属性中的每一个，如果存在 IsNotNull 谓词，我们将在谓词*之前*生成该检查。
     * 在所有这些谓词之后，我们将生成不属于其他谓词的其余 IsNotNull 检查。
     * 这具有不进行冗余 IsNotNull 检查并更好地利用短路的特性，在需要之前不加载属性。 这是非常敏感的。
     *
     * TODO: 重温这个。 我们也可以考虑重新排序谓词。
     */

    // 生成notNull检查谓词的代码
    val generatedIsNotNullChecks = new Array[Boolean](notNullPreds.length)

    // 遍历非notNull检查的其他谓词
    val generated = otherPreds.map { c =>
      // 对非notNull谓词里的列生成可能存在的Null值检查代码
      val nullChecks = c.references.map { r =>
        // 检查非notNull检查谓词条件里的引用是否存在notNull检查
        val idx = notNullPreds.indexWhere { n => n.asInstanceOf[IsNotNull].child.semanticEquals(r)}

        // idx不为-1，且generatedIsNotNullChecks中未记录，则需要处理
        if (idx != -1 && !generatedIsNotNullChecks(idx)) {
          // 记录在generatedIsNotNullChecks中
          generatedIsNotNullChecks(idx) = true
          // Use the child's output. The nullability is what the child produced.
          // 生成谓词过滤代码，输入参数分别是：notNull谓词条件、输入列的ExprCode列表，子节点输出列
          genPredicate(notNullPreds(idx), input, child.output)
        } else {
          ""
        }
      }.mkString("\n").trim

      // Here we use *this* operator's output with this output's nullability since we already
      // enforced them with the IsNotNull checks above.
      s"""
         |// [FilterExec#doConsume] 可能存在的Null值检查代码
         |$nullChecks
         |// [FilterExec#doConsume] 生成notNull谓词的判断代码
         |${genPredicate(c, input, output)}
       """.stripMargin.trim
    }.mkString("\n")

    // 剩余的notNull谓词条件单独生成判断代码
    val nullChecks = notNullPreds.zipWithIndex.map { case (c, idx) =>
      if (!generatedIsNotNullChecks(idx)) {
        genPredicate(c, input, child.output)
      } else {
        ""
      }
    }.mkString("\n")

    // Reset the isNull to false for the not-null columns, then the followed operators could
    // generate better code (remove dead branches).
    /**
     * 根据子节点的输入列，将其中存在NotNull判断的列的isNull置为false，这将为后续的操作生成更好的代码（即移除无用分支）；
     * 然后将所有输入列作为结果列列表，也将作为consume方法的第二个参数，即作为父节点的输入列。
     */
    val resultVars = input.zipWithIndex.map { case (ev, i) =>
      // 判断在notNullAttribute中是否包含遍历到的列，如果包含，则说明该列的isNull可以置为false
      if (notNullAttributes.contains(child.output(i).exprId)) {
        ev.isNull = "false"
      }
      ev
    }

    s"""
       |// [FilterExec#doConsume] 非NotNull谓词条件，可能会有NotNull判断
       |$generated
       |// [FilterExec#doConsume] 纯NotNull谓词条件
       |$nullChecks
       |// [FilterExec#doConsume] 输出度量值自增
       |$numOutput.add(1);
       |// [FilterExec#doConsume] 调用consume，继续调用父节点的doConsume
       |${consume(ctx, resultVars)}
     """.stripMargin
  }

  protected override def doExecute(): RDD[InternalRow] = {
    val numOutputRows = longMetric("numOutputRows")
    child.execute().mapPartitionsWithIndexInternal { (index, iter) =>
      // 会调用GeneratePredicate对象的generate方法生成Predicate类，完成过滤算子的逻辑。
      val predicate = newPredicate(condition, child.output)
      predicate.initialize(0)
      iter.filter { row =>
        val r = predicate.eval(row)
        if (r) numOutputRows += 1
        r
      }
    }
  }

  override def outputOrdering: Seq[SortOrder] = child.outputOrdering

  override def outputPartitioning: Partitioning = child.outputPartitioning
}

/**
 * Physical plan for sampling the dataset.
 *
 * @param lowerBound Lower-bound of the sampling probability (usually 0.0)
 * @param upperBound Upper-bound of the sampling probability. The expected fraction sampled
 *                   will be ub - lb.
 * @param withReplacement Whether to sample with replacement.
 * @param seed the random seed
 * @param child the SparkPlan
 */
case class SampleExec(
    lowerBound: Double,
    upperBound: Double,
    withReplacement: Boolean,
    seed: Long,
    child: SparkPlan) extends UnaryExecNode with CodegenSupport {
  override def output: Seq[Attribute] = child.output

  override def outputPartitioning: Partitioning = child.outputPartitioning

  override lazy val metrics = Map(
    "numOutputRows" -> SQLMetrics.createMetric(sparkContext, "number of output rows"))

  protected override def doExecute(): RDD[InternalRow] = {
    if (withReplacement) {
      // Disable gap sampling since the gap sampling method buffers two rows internally,
      // requiring us to copy the row, which is more expensive than the random number generator.
      new PartitionwiseSampledRDD[InternalRow, InternalRow](
        child.execute(),
        new PoissonSampler[InternalRow](upperBound - lowerBound, useGapSamplingIfPossible = false),
        preservesPartitioning = true,
        seed)
    } else {
      child.execute().randomSampleWithRange(lowerBound, upperBound, seed)
    }
  }

  override def inputRDDs(): Seq[RDD[InternalRow]] = {
    child.asInstanceOf[CodegenSupport].inputRDDs()
  }

  protected override def doProduce(ctx: CodegenContext): String = {
    child.asInstanceOf[CodegenSupport].produce(ctx, this)
  }

  override def doConsume(ctx: CodegenContext, input: Seq[ExprCode], row: ExprCode): String = {
    val numOutput = metricTerm(ctx, "numOutputRows")
    val sampler = ctx.freshName("sampler")

    if (withReplacement) {
      val samplerClass = classOf[PoissonSampler[UnsafeRow]].getName
      val initSampler = ctx.freshName("initSampler")
      ctx.copyResult = true
      ctx.addMutableState(s"$samplerClass<UnsafeRow>", sampler,
        s"$initSampler();")

      ctx.addNewFunction(initSampler,
        s"""
          | private void $initSampler() {
          |   $sampler = new $samplerClass<UnsafeRow>($upperBound - $lowerBound, false);
          |   java.util.Random random = new java.util.Random(${seed}L);
          |   long randomSeed = random.nextLong();
          |   int loopCount = 0;
          |   while (loopCount < partitionIndex) {
          |     randomSeed = random.nextLong();
          |     loopCount += 1;
          |   }
          |   $sampler.setSeed(randomSeed);
          | }
         """.stripMargin.trim)

      val samplingCount = ctx.freshName("samplingCount")
      s"""
         | int $samplingCount = $sampler.sample();
         | while ($samplingCount-- > 0) {
         |   $numOutput.add(1);
         |   ${consume(ctx, input)}
         | }
       """.stripMargin.trim
    } else {
      val samplerClass = classOf[BernoulliCellSampler[UnsafeRow]].getName
      ctx.addMutableState(s"$samplerClass<UnsafeRow>", sampler,
        s"""
          | $sampler = new $samplerClass<UnsafeRow>($lowerBound, $upperBound, false);
          | $sampler.setSeed(${seed}L + partitionIndex);
         """.stripMargin.trim)

      s"""
         | if ($sampler.sample() == 0) continue;
         | $numOutput.add(1);
         | ${consume(ctx, input)}
       """.stripMargin.trim
    }
  }
}


/**
 * Physical plan for range (generating a range of 64 bit numbers).
 *
 * 利用SparkContext中的parallelize方法生成给定范围内的64位数据的RDD。
 */
case class RangeExec(range: org.apache.spark.sql.catalyst.plans.logical.Range)
  extends LeafExecNode with CodegenSupport {

  def start: Long = range.start
  def step: Long = range.step
  def numSlices: Int = range.numSlices.getOrElse(sparkContext.defaultParallelism)
  def numElements: BigInt = range.numElements

  override val output: Seq[Attribute] = range.output

  override lazy val metrics = Map(
    "numOutputRows" -> SQLMetrics.createMetric(sparkContext, "number of output rows"))

  // output attributes should not affect the results
  override lazy val cleanArgs: Seq[Any] = Seq(start, step, numSlices, numElements)

  override def inputRDDs(): Seq[RDD[InternalRow]] = {
    sqlContext.sparkContext.parallelize(0 until numSlices, numSlices)
      .map(i => InternalRow(i)) :: Nil
  }

  protected override def doProduce(ctx: CodegenContext): String = {
    val numOutput = metricTerm(ctx, "numOutputRows")

    val initTerm = ctx.freshName("initRange")
    ctx.addMutableState("boolean", initTerm, s"$initTerm = false;")
    val partitionEnd = ctx.freshName("partitionEnd")
    ctx.addMutableState("long", partitionEnd, s"$partitionEnd = 0L;")
    val number = ctx.freshName("number")
    ctx.addMutableState("long", number, s"$number = 0L;")
    val overflow = ctx.freshName("overflow")
    ctx.addMutableState("boolean", overflow, s"$overflow = false;")

    val value = ctx.freshName("value")
    val ev = ExprCode("", "false", value)
    val BigInt = classOf[java.math.BigInteger].getName
    val checkEnd = if (step > 0) {
      s"$number < $partitionEnd"
    } else {
      s"$number > $partitionEnd"
    }

    ctx.addNewFunction("initRange",
      s"""
        | private void initRange(int idx) {
        |   $BigInt index = $BigInt.valueOf(idx);
        |   $BigInt numSlice = $BigInt.valueOf(${numSlices}L);
        |   $BigInt numElement = $BigInt.valueOf(${numElements.toLong}L);
        |   $BigInt step = $BigInt.valueOf(${step}L);
        |   $BigInt start = $BigInt.valueOf(${start}L);
        |
        |   $BigInt st = index.multiply(numElement).divide(numSlice).multiply(step).add(start);
        |   if (st.compareTo($BigInt.valueOf(Long.MAX_VALUE)) > 0) {
        |     $number = Long.MAX_VALUE;
        |   } else if (st.compareTo($BigInt.valueOf(Long.MIN_VALUE)) < 0) {
        |     $number = Long.MIN_VALUE;
        |   } else {
        |     $number = st.longValue();
        |   }
        |
        |   $BigInt end = index.add($BigInt.ONE).multiply(numElement).divide(numSlice)
        |     .multiply(step).add(start);
        |   if (end.compareTo($BigInt.valueOf(Long.MAX_VALUE)) > 0) {
        |     $partitionEnd = Long.MAX_VALUE;
        |   } else if (end.compareTo($BigInt.valueOf(Long.MIN_VALUE)) < 0) {
        |     $partitionEnd = Long.MIN_VALUE;
        |   } else {
        |     $partitionEnd = end.longValue();
        |   }
        |
        |   $numOutput.add(($partitionEnd - $number) / ${step}L);
        | }
       """.stripMargin)

    val input = ctx.freshName("input")
    // Right now, Range is only used when there is one upstream.
    ctx.addMutableState("scala.collection.Iterator", input, s"$input = inputs[0];")
    s"""
      | // initialize Range
      | if (!$initTerm) {
      |   $initTerm = true;
      |   initRange(partitionIndex);
      | }
      |
      | while (!$overflow && $checkEnd) {
      |  long $value = $number;
      |  $number += ${step}L;
      |  if ($number < $value ^ ${step}L < 0) {
      |    $overflow = true;
      |  }
      |  ${consume(ctx, Seq(ev))}
      |  if (shouldStop()) return;
      | }
     """.stripMargin
  }

  protected override def doExecute(): RDD[InternalRow] = {
    val numOutputRows = longMetric("numOutputRows")
    sqlContext
      .sparkContext
      .parallelize(0 until numSlices, numSlices)
      .mapPartitionsWithIndex { (i, _) =>
        val partitionStart = (i * numElements) / numSlices * step + start
        val partitionEnd = (((i + 1) * numElements) / numSlices) * step + start
        def getSafeMargin(bi: BigInt): Long =
          if (bi.isValidLong) {
            bi.toLong
          } else if (bi > 0) {
            Long.MaxValue
          } else {
            Long.MinValue
          }
        val safePartitionStart = getSafeMargin(partitionStart)
        val safePartitionEnd = getSafeMargin(partitionEnd)
        val rowSize = UnsafeRow.calculateBitSetWidthInBytes(1) + LongType.defaultSize
        val unsafeRow = UnsafeRow.createFromByteArray(rowSize, 1)

        new Iterator[InternalRow] {
          private[this] var number: Long = safePartitionStart
          private[this] var overflow: Boolean = false

          override def hasNext =
            if (!overflow) {
              if (step > 0) {
                number < safePartitionEnd
              } else {
                number > safePartitionEnd
              }
            } else false

          override def next() = {
            val ret = number
            number += step
            if (number < ret ^ step < 0) {
              // we have Long.MaxValue + Long.MaxValue < Long.MaxValue
              // and Long.MinValue + Long.MinValue > Long.MinValue, so iff the step causes a step
              // back, we are pretty sure that we have an overflow.
              overflow = true
            }

            numOutputRows += 1
            unsafeRow.setLong(0, ret)
            unsafeRow
          }
        }
      }
  }

  override def simpleString: String = range.simpleString
}

/**
 * Physical plan for unioning two plans, without a distinct. This is UNION ALL in SQL.
 */
case class UnionExec(children: Seq[SparkPlan]) extends SparkPlan {
  override def output: Seq[Attribute] =
    children.map(_.output).transpose.map(attrs =>
      attrs.head.withNullability(attrs.exists(_.nullable)))

  protected override def doExecute(): RDD[InternalRow] =
    sparkContext.union(children.map(_.execute()))
}

/**
 * Physical plan for returning a new RDD that has exactly `numPartitions` partitions.
 * Similar to coalesce defined on an [[RDD]], this operation results in a narrow dependency, e.g.
 * if you go from 1000 partitions to 100 partitions, there will not be a shuffle, instead each of
 * the 100 new partitions will claim 10 of the current partitions.
 */
case class CoalesceExec(numPartitions: Int, child: SparkPlan) extends UnaryExecNode {
  override def output: Seq[Attribute] = child.output

  override def outputPartitioning: Partitioning = {
    if (numPartitions == 1) SinglePartition
    else UnknownPartitioning(numPartitions)
  }

  protected override def doExecute(): RDD[InternalRow] = {
    child.execute().coalesce(numPartitions, shuffle = false)
  }
}

/**
 * A plan node that does nothing but lie about the output of its child.  Used to spice a
 * (hopefully structurally equivalent) tree from a different optimization sequence into an already
 * resolved tree.
 */
case class OutputFakerExec(output: Seq[Attribute], child: SparkPlan) extends SparkPlan {
  def children: Seq[SparkPlan] = child :: Nil

  protected override def doExecute(): RDD[InternalRow] = child.execute()
}

/**
 * Physical plan for a subquery.
 */
case class SubqueryExec(name: String, child: SparkPlan) extends UnaryExecNode {

  override lazy val metrics = Map(
    "dataSize" -> SQLMetrics.createMetric(sparkContext, "data size (bytes)"),
    "collectTime" -> SQLMetrics.createMetric(sparkContext, "time to collect (ms)"))

  override def output: Seq[Attribute] = child.output

  override def outputPartitioning: Partitioning = child.outputPartitioning

  override def outputOrdering: Seq[SortOrder] = child.outputOrdering

  override def sameResult(o: SparkPlan): Boolean = o match {
    case s: SubqueryExec => child.sameResult(s.child)
    case _ => false
  }

  @transient
  private lazy val relationFuture: Future[Array[InternalRow]] = {
    // relationFuture is used in "doExecute". Therefore we can get the execution id correctly here.
    val executionId = sparkContext.getLocalProperty(SQLExecution.EXECUTION_ID_KEY)
    Future {
      // This will run in another thread. Set the execution id so that we can connect these jobs
      // with the correct execution.
      SQLExecution.withExecutionId(sparkContext, executionId) {
        val beforeCollect = System.nanoTime()
        // Note that we use .executeCollect() because we don't want to convert data to Scala types
        val rows: Array[InternalRow] = child.executeCollect()
        val beforeBuild = System.nanoTime()
        longMetric("collectTime") += (beforeBuild - beforeCollect) / 1000000
        val dataSize = rows.map(_.asInstanceOf[UnsafeRow].getSizeInBytes.toLong).sum
        longMetric("dataSize") += dataSize

        // There are some cases we don't care about the metrics and call `SparkPlan.doExecute`
        // directly without setting an execution id. We should be tolerant to it.
        if (executionId != null) {
          sparkContext.listenerBus.post(SparkListenerDriverAccumUpdates(
            executionId.toLong, metrics.values.map(m => m.id -> m.value).toSeq))
        }

        rows
      }
    }(SubqueryExec.executionContext)
  }

  protected override def doPrepare(): Unit = {
    relationFuture
  }

  protected override def doExecute(): RDD[InternalRow] = {
    child.execute()
  }

  override def executeCollect(): Array[InternalRow] = {
    ThreadUtils.awaitResultInForkJoinSafely(relationFuture, Duration.Inf)
  }
}

object SubqueryExec {
  private[execution] val executionContext = ExecutionContext.fromExecutorService(
    ThreadUtils.newDaemonCachedThreadPool("subquery", 16))
}
