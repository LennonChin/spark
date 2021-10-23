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

import org.apache.spark.{broadcast, TaskContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.codegen._
import org.apache.spark.sql.catalyst.plans.physical.Partitioning
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.aggregate.HashAggregateExec
import org.apache.spark.sql.execution.joins.{BroadcastHashJoinExec, SortMergeJoinExec}
import org.apache.spark.sql.execution.metric.SQLMetrics
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types._
import org.apache.spark.util.Utils

/**
 * An interface for those physical operators that support codegen.
 * 支持Codegen的物理操作
 */
trait CodegenSupport extends SparkPlan {

  /**
   * Prefix used in the current operator's variable names.
   * 表示对应的物理算子节点生成的代码中变量名的前缀。不同的节点类型其前缀不同。
   **/
  private def variablePrefix: String = this match {
    case _: HashAggregateExec => "agg"
    case _: BroadcastHashJoinExec => "bhj"
    case _: SortMergeJoinExec => "smj"
    case _: RDDScanExec => "rdd"
    case _: DataSourceScanExec => "scan"
    case _ => nodeName.toLowerCase
  }

  /**
   * Creates a metric using the specified name.
   *
   * @return name of the variable representing the metric
   */
  def metricTerm(ctx: CodegenContext, name: String): String = {
    ctx.addReferenceObj(name, longMetric(name))
  }

  /**
   * Whether this SparkPlan support whole stage codegen or not.
   * 判断是否支持代码生成。
   */
  def supportCodegen: Boolean = true

  /**
   * Which SparkPlan is calling produce() of this one. It's itself for the first SparkPlan.
   */
  protected var parent: CodegenSupport = null

  /**
   * Returns all the RDDs of InternalRow which generates the input rows.
   *
   * Note: right now we support up to two RDDs.
   *
   * 得产生输入数据的inputRDDs。
   */
  def inputRDDs(): Seq[RDD[InternalRow]]

  /**
   * Returns Java source code to process the rows from input RDD.
   *
   * 返回的是该节点及其子节点所生成的代码。会调用doProduce方法。
   */
  final def produce(ctx: CodegenContext, parent: CodegenSupport): String = executeQuery {
    this.parent = parent
    ctx.freshNamePrefix = variablePrefix
    s"""
       |${ctx.registerComment(s"PRODUCE: ${this.simpleString}")}
       |${doProduce(ctx)}
     """.stripMargin
  }

  /**
   * Generate the Java source code to process, should be overridden by subclass to support codegen.
   *
   * doProduce() usually generate the framework, for example, aggregation could generate this:
   *
   *   if (!initialized) {
   *     # create a hash map, then build the aggregation hash map
   *     # call child.produce()
   *     initialized = true;
   *   }
   *   while (hashmap.hasNext()) {
   *     row = hashmap.next();
   *     # build the aggregation results
   *     # create variables for results
   *     # call consume(), which will call parent.doConsume()
   *      if (shouldStop()) return;
   *   }
   *
   * 生成Java源代码，该方法需要由子类实现以支持Codegen。
   * doProduce()一般用来生成代码框架。
   */
  protected def doProduce(ctx: CodegenContext): String

  /**
   * Consume the generated columns or row from current SparkPlan, call its parent's `doConsume()`.
   *
   * 返回的是该CodegenSupport节点处理数据核心逻辑所对应生成的代码。会调用其父节点的doConsume方法。
   *
   * 每个物理算子节点的consume方法将生成相应的代码来完成该节点的数据处理逻辑。
   * consume方法将递归调用其父节点的doConsume方法，这样正好对应了子节点处理逻辑先于父节点处理逻辑的顺序关系。
   *
   * @param outputVars 子节点输出列的ExprCode列表（Seq[ExprCode]）
   * @param row 表示当前数据行对应的变量（ExprCode），row为null，说明可能输入行是UnsafeRow。
   * @return
   */
  final def consume(ctx: CodegenContext, outputVars: Seq[ExprCode], row: String = null): String = {
    // 下一步逻辑处理的变量inputVars，类型为Seq[ExprCode]，不同的变量代表不同的列。
    val inputVars: Seq[ExprCode] =
      if (row != null) { // 如果有行变量
        ctx.currentVars = null
        ctx.INPUT_ROW = row // 将CodegenContext对象的INPUT_ROW指向该行变量
        // 返回值为该节点的输出字段对应的BoundReference生成的代码
        output.zipWithIndex.map { case (attr, i) =>
          BoundReference(i, attr.dataType, attr.nullable).genCode(ctx)
        }
      } else {
        assert(outputVars != null)
        assert(outputVars.length == output.length)
        // outputVars will be used to generate the code for UnsafeRow, so we should copy them
        // 对outputVars执行copy操作，因为这里的outputVars变量会用到CodegenContext中的currentVars来生成UnsafeRow代码。
        outputVars.map(_.copy())
      }

    // 生成rowVar，类型为ExprCode，代表整行数据的变量名。
    val rowVar = if (row != null) {
      // 如果传入的行变量不为空，则直接对应该行变量的ExprCode对象
      ExprCode("", "false", row)
    } else {
      // 根据输出列决定返回的ExprCode
      if (outputVars.nonEmpty) { // 行变量row为空，但是传入的列变量列表outputVars不为空

        // 将节点原始输出构造为BoundReference列表，UnsafeProjection需要使用
        val colExprs = output.zipWithIndex.map { case (attr, i) =>
          BoundReference(i, attr.dataType, attr.nullable)
        }

        /**
         * evaluateVariables方法可以得到传入的Seq[ExprCode]中所有code不为空的ExprCode代码，
         * 按行分隔，并将ExprCode对应的code设置为空。
         *
         * 因此该行代码可以获取所有输出列的代码，按行分隔。
         */
        val evaluateInputs = evaluateVariables(outputVars)

        // generate the code to create a UnsafeRow
        // 拼装创建UnsafeRow的代码
        ctx.INPUT_ROW = row
        ctx.currentVars = outputVars

        // 创建产生UnsafeRow的UnsafeProjection
        val ev = GenerateUnsafeProjection.createCode(ctx, colExprs, false)

        // 拼装UnsafeProjection的代码
        val code = s"""
          |$evaluateInputs
          |${ev.code.trim}
         """.stripMargin.trim

        // 返回ExprCode
        ExprCode(code, "false", ev.value)
      } else { // 行变量row为空，传入的列变量列表outputVars也为空，构造名为unsafeRow的ExprCode对象。
        // There is no columns
        ExprCode("", "false", "unsafeRow")
      }
    }

    ctx.freshNamePrefix = parent.variablePrefix // 更新Context中的Prefix为父节点的Prefix

    /**
     * evaluateRequiredVariables方法会根据所需的列集合（第三个参数，AttributeSet类型）筛选出对应的ExprCode代码，
     * 其他操作和evaluateVariables方法中的逻辑相同。
     *
     * 该行代码会拼装需要用到的输出列的生成代码，按行分隔。
     */
    val evaluated = evaluateRequiredVariables(output, inputVars, parent.usedInputs)

    // 递归调用父节点的doConsume方法，将代码进行拼装
    s"""
       |// [CodegenSupport#consume start] ${this.getClass.getName}
       |${ctx.registerComment(s"CONSUME: ${parent.simpleString}")}
       |// [CodegenSupport#consume evaluated] ${this.getClass.getName}
       |$evaluated
       |// [CodegenSupport#consume parent.doConsume] ${parent.getClass.getName}
       |${parent.doConsume(ctx, inputVars, rowVar)}
       |// [CodegenSupport#consume end] ${this.getClass.getName}
     """.stripMargin
  }

  /**
   * Returns source code to evaluate all the variables, and clear the code of them, to prevent
   * them to be evaluated twice.
   *
   * 得到按行分隔的所有code不为空的ExprCode代码，并将ExprCode对应的code设置为空
   */
  protected def evaluateVariables(variables: Seq[ExprCode]): String = {
    val evaluate = variables.filter(_.code != "").map(_.code.trim).mkString("\n")
    variables.foreach(_.code = "")
    evaluate
  }

  /**
   * Returns source code to evaluate the variables for required attributes, and clear the code
   * of evaluated variables, to prevent them to be evaluated twice.
   *
   * 根据所需的列集合（AttributeSet）筛选出对应的ExprCode代码，其他操作和evaluateVariables方法中的逻辑相同。
   */
  protected def evaluateRequiredVariables(
      attributes: Seq[Attribute],
      variables: Seq[ExprCode],
      required: AttributeSet): String = {
    val evaluateVars = new StringBuilder
    variables.zipWithIndex.foreach { case (ev, i) =>
      if (ev.code != "" && required.contains(attributes(i))) {
        evaluateVars.append(ev.code.trim + "\n")
        ev.code = ""
      }
    }
    evaluateVars.toString()
  }

  /**
   * The subset of inputSet those should be evaluated before this plan.
   *
   * We will use this to insert some code to access those columns that are actually used by current
   * plan before calling doConsume().
   */
  def usedInputs: AttributeSet = references

  /**
   * Generate the Java source code to process the rows from child SparkPlan.
   *
   * This should be override by subclass to support codegen.
   *
   * For example, Filter will generate the code like this:
   *
   *   # code to evaluate the predicate expression, result is isNull1 and value2
   *   if (isNull1 || !value2) continue;
   *   # call consume(), which will call parent.doConsume()
   *
   * Note: A plan can either consume the rows as UnsafeRow (row), or a list of variables (input).
   */
  def doConsume(ctx: CodegenContext, input: Seq[ExprCode], row: ExprCode): String = {
    throw new UnsupportedOperationException
  }
}


/**
 * InputAdapter is used to hide a SparkPlan from a subtree that support codegen.
 *
 * This is the leaf node of a tree with WholeStageCodegen that is used to generate code
 * that consumes an RDD iterator of InternalRow.
 *
 * InputAdapter 用于从支持代码生成的子树中隐藏 SparkPlan。
 * 这是带有 WholeStageCodegen 的树的叶节点，用于生成使用 InternalRow 的 RDD 迭代器的代码。
 */
case class InputAdapter(child: SparkPlan) extends UnaryExecNode with CodegenSupport {

  override def output: Seq[Attribute] = child.output

  override def outputPartitioning: Partitioning = child.outputPartitioning

  override def outputOrdering: Seq[SortOrder] = child.outputOrdering

  override def doExecute(): RDD[InternalRow] = {
    child.execute()
  }

  override def doExecuteBroadcast[T](): broadcast.Broadcast[T] = {
    child.doExecuteBroadcast()
  }

  override def inputRDDs(): Seq[RDD[InternalRow]] = {
    child.execute() :: Nil
  }

  override def doProduce(ctx: CodegenContext): String = {
    val input = ctx.freshName("input")
    // Right now, InputAdapter is only used when there is one input RDD.
    ctx.addMutableState("scala.collection.Iterator", input, s"$input = inputs[0];")
    val row = ctx.freshName("row")
    s"""
       | // [InputAdapter.doProduce start] 不断迭代输入数据
       | while ($input.hasNext()) {
       |   // [InputAdapter.doProduce] 获取下一行数据
       |   InternalRow $row = (InternalRow) $input.next();
       |   // [InputAdapter.doProduce] 交给consume方法处理
       |   ${consume(ctx, null, row).trim}
       |   if (shouldStop()) return; // [InputAdapter.doProduce]
       | }
       | // [InputAdapter.doProduce end]
     """.stripMargin
  }

  override def generateTreeString(
      depth: Int,
      lastChildren: Seq[Boolean],
      builder: StringBuilder,
      verbose: Boolean,
      prefix: String = ""): StringBuilder = {
    child.generateTreeString(depth, lastChildren, builder, verbose, "")
  }
}

object WholeStageCodegenExec {
  val PIPELINE_DURATION_METRIC = "duration"
}

/**
 * WholeStageCodegen compile a subtree of plans that support codegen together into single Java
 * function.
 *
 * Here is the call graph of to generate Java source (plan A support codegen, but plan B does not):
 *
 *   WholeStageCodegen       Plan A               FakeInput        Plan B
 * =========================================================================
 *
 * -> execute()
 *     |
 *  doExecute() --------->   inputRDDs() -------> inputRDDs() ------> execute()
 *     |
 *     +----------------->   produce()
 *                             |
 *                          doProduce()  -------> produce()
 *                                                   |
 *                                                doProduce()
 *                                                   |
 *                         doConsume() <--------- consume()
 *                             |
 *  doConsume()  <--------  consume()
 *
 * SparkPlan A should override doProduce() and doConsume().
 *
 * doCodeGen() will create a CodeGenContext, which will hold a list of variables for input,
 * used to generated code for BoundReference.
 */
case class WholeStageCodegenExec(child: SparkPlan) extends UnaryExecNode with CodegenSupport {

  override def output: Seq[Attribute] = child.output

  override def outputPartitioning: Partitioning = child.outputPartitioning

  override def outputOrdering: Seq[SortOrder] = child.outputOrdering

  override lazy val metrics = Map(
    "pipelineTime" -> SQLMetrics.createTimingMetric(sparkContext,
      WholeStageCodegenExec.PIPELINE_DURATION_METRIC))

  /**
   * Generates code for this subtree.
   *
   * 生成代码的入口
   *
   * @return the tuple of the codegen context and the actual generated source.
   */
  def doCodeGen(): (CodegenContext, CodeAndComment) = {
    // 构造CodoegenContext。
    val ctx = new CodegenContext
    // 将此对象作为CodegenSupport中produce方法的参数，直接调用子节点的produce方法生成具体的处理代码片段code。
    val code = child.asInstanceOf[CodegenSupport].produce(ctx, this)
    // 基于code代码片段和代码生成之后的CodegenContext对象，构造完整的代码段。
    val source = s"""
      // 静态方法，用于构造GeneratedIterator对象
      public Object generate(Object[] references) {
        return new GeneratedIterator(references);
      }

      ${ctx.registerComment(s"""Codegend pipeline for\n${child.treeString.trim}""")} // 注释
      final class GeneratedIterator extends org.apache.spark.sql.execution.BufferedRowIterator {

        private Object[] references;
        private scala.collection.Iterator[] inputs;
        ${ctx.declareMutableStates()} // 变量定义

        public GeneratedIterator(Object[] references) {
          this.references = references;
        }

        // 负责相关变量的初始化
        public void init(int index, scala.collection.Iterator[] inputs) {
          partitionIndex = index;
          this.inputs = inputs;
          ${ctx.initMutableStates()} // 遍历初始化
          ${ctx.initPartition()}
        }

        ${ctx.declareAddedFunctions()} // 声明辅助函数

        // 用于循环处理RDD中的数据行
        protected void processNext() throws java.io.IOException {
          ${code.trim} // 实际代码
        }
      }
      """.trim

    // try to compile, helpful for debug
    val cleanedSource = CodeFormatter.stripOverlappingComments(
      new CodeAndComment(CodeFormatter.stripExtraNewLines(source), ctx.getPlaceHolderToComments()))

    logDebug(s"\n${CodeFormatter.format(cleanedSource)}")
    (ctx, cleanedSource)
  }

  override def doExecute(): RDD[InternalRow] = {
    // Codegen生成代码
    val (ctx, cleanedSource) = doCodeGen()
    // try to compile and fallback if it failed
    try {
      // 尝试使用Janino编译，内部有缓存机制，不会重复编译
      CodeGenerator.compile(cleanedSource)
    } catch {
      // 如果编译失败且配置回退机制（参数spark.sql.codegen.fallback默认为true），则代码生成将被舍弃转而执行Spark原生的逻辑。
      case e: Exception if !Utils.isTesting && sqlContext.conf.wholeStageFallback => // spark.sql.codegen.fallback
        // We should already saw the error message
        logWarning(s"Whole-stage codegen disabled for this plan:\n $treeString")
        return child.execute()
    }
    val references = ctx.references.toArray

    val durationMs = longMetric("pipelineTime")

    // 调用inputRDDs方法得到RDD列表后，会根据RDD的数量采取不同的处理逻辑。
    val rdds = child.asInstanceOf[CodegenSupport].inputRDDs()
    // 代码生成最多支持对两个RDD进行处理
    assert(rdds.size <= 2, "Up to two input RDDs can be supported")

    if (rdds.length == 1) {
      rdds.head.mapPartitionsWithIndex { (index, iter) =>
        val clazz = CodeGenerator.compile(cleanedSource) // 得到GeneratedClass
        // 调用generate得到BufferedRowIterator
        val buffer = clazz.generate(references).asInstanceOf[BufferedRowIterator]
        buffer.init(index, Array(iter))
        new Iterator[InternalRow] {
          override def hasNext: Boolean = {
            val v = buffer.hasNext
            if (!v) durationMs += buffer.durationMs()
            v
          }
          override def next: InternalRow = buffer.next()
        }
      }
    } else {
      // Right now, we support up to two input RDDs.
      rdds.head.zipPartitions(rdds(1)) { (leftIter, rightIter) =>
        Iterator((leftIter, rightIter))
        // a small hack to obtain the correct partition index
      }.mapPartitionsWithIndex { (index, zippedIter) =>
        val (leftIter, rightIter) = zippedIter.next()
        val clazz = CodeGenerator.compile(cleanedSource)
        val buffer = clazz.generate(references).asInstanceOf[BufferedRowIterator]
        buffer.init(index, Array(leftIter, rightIter))
        new Iterator[InternalRow] {
          override def hasNext: Boolean = {
            val v = buffer.hasNext
            if (!v) durationMs += buffer.durationMs()
            v
          }
          override def next: InternalRow = buffer.next()
        }
      }
    }
  }

  override def inputRDDs(): Seq[RDD[InternalRow]] = {
    throw new UnsupportedOperationException
  }

  override def doProduce(ctx: CodegenContext): String = {
    throw new UnsupportedOperationException
  }

  override def doConsume(ctx: CodegenContext, input: Seq[ExprCode], row: ExprCode): String = {
    // 决定结果是否需要复制
    val doCopy = if (ctx.copyResult) {
      ".copy()"
    } else {
      ""
    }
    s"""
      |// [WholeStageCodegenExec#doConsume] 首先输出row变量的代码，对应于子节点的输入列
      |${row.code}
      |// [WholeStageCodegenExec#doConsume] 添加结果
      |append(${row.value}$doCopy);
     """.stripMargin.trim
  }

  override def generateTreeString(
      depth: Int,
      lastChildren: Seq[Boolean],
      builder: StringBuilder,
      verbose: Boolean,
      prefix: String = ""): StringBuilder = {
    child.generateTreeString(depth, lastChildren, builder, verbose, "*")
  }
}


/**
 * Find the chained plans that support codegen, collapse them together as WholeStageCodegen.
 *
 * 找出支持Codegen的链条计划，将它们折叠为一个WholeStageCodegen节点，用于全阶段代码生成
 */
case class CollapseCodegenStages(conf: SQLConf) extends Rule[SparkPlan] {

  private def supportCodegen(e: Expression): Boolean = e match {
    // 叶子节点表达式支持Codegen
    case e: LeafExpression => true
    // CodegenFallback requires the input to be an InternalRow
    /**
     * CodegenFallback类型的表达式要求输入时InternalRow类型。
     * 实现了CodegenFallback特质的表达式不支持Codegen。
     */
    case e: CodegenFallback => false
    case _ => true
  }

  // 递归调用计算嵌套的字段数量
  private def numOfNestedFields(dataType: DataType): Int = dataType match {
    case dt: StructType => dt.fields.map(f => numOfNestedFields(f.dataType)).sum
    case m: MapType => numOfNestedFields(m.keyType) + numOfNestedFields(m.valueType)
    case a: ArrayType => numOfNestedFields(a.elementType)
    case u: UserDefinedType[_] => numOfNestedFields(u.sqlType)
    case _ => 1
  }

  private def supportCodegen(plan: SparkPlan): Boolean = plan match {
    case plan: CodegenSupport if plan.supportCodegen => // plan需要是CodegenSupport的子类，同时支持Codegen
      // plan中是否存在不支持Codegen的表达式
      val willFallback = plan.expressions.exists(_.find(e => !supportCodegen(e)).isDefined)
      // the generated code will be huge if there are too many columns
      // plan是否太多输出字段
      val hasTooManyOutputFields =
        numOfNestedFields(plan.schema) > conf.wholeStageMaxNumFields // spark.sql.codegen.maxFields，默认100
      // plan是否太多输入字段
      val hasTooManyInputFields =
        plan.children.map(p => numOfNestedFields(p.schema)).exists(_ > conf.wholeStageMaxNumFields) // spark.sql.codegen.maxFields，默认100

      // 不存在不支持Codegen的表达式，输入输出列都不超过限制
      !willFallback && !hasTooManyOutputFields && !hasTooManyInputFields
    case _ => false
  }

  /**
   * Inserts an InputAdapter on top of those that do not support codegen.
   */
  private def insertInputAdapter(plan: SparkPlan): SparkPlan = plan match {
    // InnerLike Join支持Codegen，其他不支持
    case j @ SortMergeJoinExec(_, _, _, _, left, right) if j.supportCodegen =>
      // The children of SortMergeJoin should do codegen separately.
      // 在左右子节点上层包装一个InputAdapter节点
      j.copy(left = InputAdapter(insertWholeStageCodegen(left)),
        right = InputAdapter(insertWholeStageCodegen(right)))
    case p if !supportCodegen(p) => // p不支持Codegen
      // collapse them recursively
      // 在p节点上层包装一个InputAdapter节点
      InputAdapter(insertWholeStageCodegen(p))
    case p =>
      // 其他情况，给p所有子节点上层包装一个InputAdapter节点
      p.withNewChildren(p.children.map(insertInputAdapter))
  }

  /**
   * Inserts a WholeStageCodegen on top of those that support codegen.
   *
   * 在支持Codegen的节点上添加WholeStageCodegen节点，递归调用。
   */
  private def insertWholeStageCodegen(plan: SparkPlan): SparkPlan = plan match {
    // For operators that will output domain object, do not insert WholeStageCodegen for it as
    // domain object can not be written into unsafe row.
    // 计划只有一个ObjectType类型输出列，递归判断子节点是否可以插入WholeStageCodegen节点
    case plan if plan.output.length == 1 && plan.output.head.dataType.isInstanceOf[ObjectType] =>
      plan.withNewChildren(plan.children.map(insertWholeStageCodegen))
    case plan: CodegenSupport if supportCodegen(plan) =>
      // 插入WholeStageCodegenExec节点，递归判断子节点是否需要插入InputAdapter节点
      WholeStageCodegenExec(insertInputAdapter(plan))
    case other =>
      // 递归判断子节点是否可以插入WholeStageCodegen节点
      other.withNewChildren(other.children.map(insertWholeStageCodegen))
  }

  def apply(plan: SparkPlan): SparkPlan = {
    if (conf.wholeStageEnabled) { // spark.sql.codegen.wholeStage，默认为true
      // 插入WholeStageCodegen节点
      insertWholeStageCodegen(plan)
    } else {
      plan
    }
  }
}
