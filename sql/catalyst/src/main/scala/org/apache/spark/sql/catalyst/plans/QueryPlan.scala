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

package org.apache.spark.sql.catalyst.plans

import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.trees.TreeNode
import org.apache.spark.sql.types.{DataType, StructType}

/**
 * QueryPlan类下面包含逻辑算子树（LogicalPlan）和物理执行算子树（SparkPlan）两个重要的子类，
 * 其中逻辑算子树在Catalyst中内置实现，可以剥离出来直接应用到其他系统中；
 * 而物理算子树SparkPlan和Spark执行层紧密相关，当Catalyst应用到其他计算模型时，可以进行相应的适配修改。
 *
 * @tparam PlanType
 */
abstract class QueryPlan[PlanType <: QueryPlan[PlanType]] extends TreeNode[PlanType] {
  self: PlanType =>

  // 具体内容由不同子节点实现
  def output: Seq[Attribute]

  /**
   * Extracts the relevant constraints from a given set of constraints based on the attributes that
   * appear in the [[outputSet]].
   */
  protected def getRelevantConstraints(constraints: Set[Expression]): Set[Expression] = {
    constraints
      .union(inferAdditionalConstraints(constraints))
      .union(constructIsNotNullConstraints(constraints)) // 添加某些字段的Not Null约束
      .filter(constraint =>
        constraint.references.nonEmpty && constraint.references.subsetOf(outputSet) &&
          constraint.deterministic)
  }

  /**
   * Infers a set of `isNotNull` constraints from null intolerant expressions as well as
   * non-nullable attributes. For e.g., if an expression is of the form (`a > 5`), this
   * returns a constraint of the form `isNotNull(a)`
   */
  private def constructIsNotNullConstraints(constraints: Set[Expression]): Set[Expression] = {
    // First, we propagate constraints from the null intolerant expressions.
    var isNotNullConstraints: Set[Expression] = constraints.flatMap(inferIsNotNullConstraints)

    // Second, we infer additional constraints from non-nullable attributes that are part of the
    // operator's output
    val nonNullableAttributes = output.filterNot(_.nullable)
    isNotNullConstraints ++= nonNullableAttributes.map(IsNotNull).toSet

    isNotNullConstraints -- constraints
  }

  /**
   * Infer the Attribute-specific IsNotNull constraints from the null intolerant child expressions
   * of constraints.
   *
   * 推断IsNotNull的约束
   */
  private def inferIsNotNullConstraints(constraint: Expression): Seq[Expression] =
    constraint match {
      // When the root is IsNotNull, we can push IsNotNull through the child null intolerant
      // expressions
      // 如果根就是IsNotNull，则推断它的表达式以及子表达式
      case IsNotNull(expr) => scanNullIntolerantAttribute(expr).map(IsNotNull(_))
      // Constraints always return true for all the inputs. That means, null will never be returned.
      // Thus, we can infer `IsNotNull(constraint)`, and also push IsNotNull through the child
      // null intolerant expressions.
      // 否则直接从根开始推断
      case _ => scanNullIntolerantAttribute(constraint).map(IsNotNull(_))
    }

  /**
   * Recursively explores the expressions which are null intolerant and returns all attributes
   * in these expressions.
   *
   * 递归排查表达式，并且找出需要保证不为Null的列属性，返回这些表达式中所有的属性。
   */
  private def scanNullIntolerantAttribute(expr: Expression): Seq[Attribute] = expr match {
    case a: Attribute => Seq(a) // 如果是Attribute，直接返回
    case _: NullIntolerant => expr.children.flatMap(scanNullIntolerantAttribute) // 排查子节点
    case _ => Seq.empty[Attribute]
  }

  /**
   * Collect aliases from expressions, so we may avoid producing recursive constraints.
   *
   * 记录节点与子节点表达式中所有的别名信息
   */
  private lazy val aliasMap = AttributeMap(
    (expressions ++ children.flatMap(_.expressions)).collect {
      case a: Alias => (a.toAttribute, a.child)
    })

  /**
   * Infers an additional set of constraints from a given set of equality constraints.
   * For e.g., if an operator has constraints of the form (`a = 5`, `a = b`), this returns an
   * additional constraint of the form `b = 5`.
   *
   * [SPARK-17733] We explicitly prevent producing recursive constraints of the form `a = f(a, b)`
   * as they are often useless and can lead to a non-converging set of constraints.
   *
   * 从一组给定的等式约束中推断出一组额外的约束。
   * 例如，如果一个操作有一组约束（a = 5，a = b），那么需要返回附加的约束 b = 5。
   */
  private def inferAdditionalConstraints(constraints: Set[Expression]): Set[Expression] = {
    val constraintClasses = generateEquivalentConstraintClasses(constraints)

    var inferredConstraints = Set.empty[Expression]
    constraints.foreach {
      case eq @ EqualTo(l: Attribute, r: Attribute) => // 提取 = 左右的属性
        val candidateConstraints = constraints - eq
        inferredConstraints ++= candidateConstraints.map(_ transform {
          case a: Attribute if a.semanticEquals(l) &&
            !isRecursiveDeduction(r, constraintClasses) => r
        })
        inferredConstraints ++= candidateConstraints.map(_ transform {
          case a: Attribute if a.semanticEquals(r) &&
            !isRecursiveDeduction(l, constraintClasses) => l
        })
      case _ => // No inference
    }
    inferredConstraints -- constraints
  }

  /*
   * Generate a sequence of expression sets from constraints, where each set stores an equivalence
   * class of expressions. For example, Set(`a = b`, `b = c`, `e = f`) will generate the following
   * expression sets: (Set(a, b, c), Set(e, f)). This will be used to search all expressions equal
   * to an selected attribute.
   */
  private def generateEquivalentConstraintClasses(
      constraints: Set[Expression]): Seq[Set[Expression]] = {
    var constraintClasses = Seq.empty[Set[Expression]]
    constraints.foreach {
      case eq @ EqualTo(l: Attribute, r: Attribute) =>
        // Transform [[Alias]] to its child.
        val left = aliasMap.getOrElse(l, l)
        val right = aliasMap.getOrElse(r, r)
        // Get the expression set for an equivalence constraint class.
        val leftConstraintClass = getConstraintClass(left, constraintClasses)
        val rightConstraintClass = getConstraintClass(right, constraintClasses)
        if (leftConstraintClass.nonEmpty && rightConstraintClass.nonEmpty) {
          // Combine the two sets.
          constraintClasses = constraintClasses
            .diff(leftConstraintClass :: rightConstraintClass :: Nil) :+
            (leftConstraintClass ++ rightConstraintClass)
        } else if (leftConstraintClass.nonEmpty) { // && rightConstraintClass.isEmpty
          // Update equivalence class of `left` expression.
          constraintClasses = constraintClasses
            .diff(leftConstraintClass :: Nil) :+ (leftConstraintClass + right)
        } else if (rightConstraintClass.nonEmpty) { // && leftConstraintClass.isEmpty
          // Update equivalence class of `right` expression.
          constraintClasses = constraintClasses
            .diff(rightConstraintClass :: Nil) :+ (rightConstraintClass + left)
        } else { // leftConstraintClass.isEmpty && rightConstraintClass.isEmpty
          // Create new equivalence constraint class since neither expression presents
          // in any classes.
          constraintClasses = constraintClasses :+ Set(left, right)
        }
      case _ => // Skip
    }

    constraintClasses
  }

  /*
   * Get all expressions equivalent to the selected expression.
   */
  private def getConstraintClass(
      expr: Expression,
      constraintClasses: Seq[Set[Expression]]): Set[Expression] =
    constraintClasses.find(_.contains(expr)).getOrElse(Set.empty[Expression])

  /*
   *  Check whether replace by an [[Attribute]] will cause a recursive deduction. Generally it
   *  has the form like: `a -> f(a, b)`, where `a` and `b` are expressions and `f` is a function.
   *  Here we first get all expressions equal to `attr` and then check whether at least one of them
   *  is a child of the referenced expression.
   */
  private def isRecursiveDeduction(
      attr: Attribute,
      constraintClasses: Seq[Set[Expression]]): Boolean = {
    val expr = aliasMap.getOrElse(attr, attr)
    getConstraintClass(expr, constraintClasses).exists { e =>
      expr.children.exists(_.semanticEquals(e))
    }
  }

  /**
   * An [[ExpressionSet]] that contains invariants about the rows output by this operator. For
   * example, if this set contains the expression `a = 2` then that expression is guaranteed to
   * evaluate to `true` for all rows produced.
   *
   * ExpressionSet包含了当前算子操作输出行的不变量。
   * 例如：如果这个集合包含表达式 a = 2，那么对于生成的所有行，该表达式保证计算为true。
   */
  lazy val constraints: ExpressionSet = ExpressionSet(getRelevantConstraints(validConstraints))

  /**
   * This method can be overridden by any child class of QueryPlan to specify a set of constraints
   * based on the given operator's constraint propagation logic. These constraints are then
   * canonicalized and filtered automatically to contain only those attributes that appear in the
   * [[outputSet]].
   *
   * See [[Canonicalize]] for more details.
   */
  protected def validConstraints: Set[Expression] = Set.empty

  /**
   * Returns the set of attributes that are output by this node.
   *
   * 将output的返回值进行封装，得到AttributeSet集合类型的结果
   */
  def outputSet: AttributeSet = AttributeSet(output)

  /**
   * All Attributes that appear in expressions from this operator.  Note that this set does not
   * include attributes that are implicitly referenced by being passed through to the output tuple.
   *
   * 表示节点表达式中所涉及的所有属性集合
   */
  def references: AttributeSet = AttributeSet(expressions.flatMap(_.references))

  /**
   * The set of all attributes that are input to this operator by its children.
   *
   * 节点的输入属性对应所有子节点的输出
   */
  def inputSet: AttributeSet =
    AttributeSet(children.flatMap(_.asInstanceOf[QueryPlan[PlanType]].output))

  /**
   * The set of all attributes that are produced by this node.
   *
   * 表示该节点所产生的属性
   */
  def producedAttributes: AttributeSet = AttributeSet.empty

  /**
   * Attributes that are referenced by expressions but not provided by this nodes children.
   * Subclasses should override this method if they produce attributes internally as it is used by
   * assertions designed to prevent the construction of invalid plans.
   *
   * 表示该节点表达式中涉及的但是其子节点输出中并不包含的属性
   */
  def missingInput: AttributeSet = references -- inputSet -- producedAttributes

  /**
   * Runs [[transform]] with `rule` on all expressions present in this query operator.
   * Users should not expect a specific directionality. If a specific directionality is needed,
   * transformExpressionsDown or transformExpressionsUp should be used.
   *
   * 用户不应对此方法要求特定的遍历方向性。
   * 如果需要特定的遍历方向性，则应使用transformExpressionsDown或transformExpressionsUp。
   * 该方法默认是自顶向下进行遍历，即先序遍历。
   *
   * @param rule the rule to be applied to every expression in this operator.
   */
  def transformExpressions(rule: PartialFunction[Expression, Expression]): this.type = {
    transformExpressionsDown(rule)
  }

  /**
   * Runs [[transformDown]] with `rule` on all expressions present in this query operator.
   *
   * 自顶向下对该操作中的所有表达式进行规则应用。
   *
   * @param rule the rule to be applied to every expression in this operator.
   */
  def transformExpressionsDown(rule: PartialFunction[Expression, Expression]): this.type = {
    var changed = false

    @inline def transformExpressionDown(e: Expression): Expression = {
      val newE = e.transformDown(rule) // 自顶向下运用规则
      if (newE.fastEquals(e)) {
        e
      } else {
        changed = true
        newE
      }
    }

    def recursiveTransform(arg: Any): AnyRef = arg match {
      case e: Expression => transformExpressionDown(e) // 表达式，直接调用transformExpressionDown
      case Some(e: Expression) => Some(transformExpressionDown(e)) // 表达式，直接调用transformExpressionDown
      case m: Map[_, _] => m
      case d: DataType => d // Avoid unpacking Structs
      case seq: Traversable[_] => seq.map(recursiveTransform) // 递归集合内的每个元素
      case other: AnyRef => other
      case null => null
    }

    /**
     * 对所有节点进行transformDown操作。
     * 此处的mapProductIterator会对当前QueryPlan实例的构造参数进行遍历，
     * 传递给recursiveTransform进行模式匹配并做相应的操作。
     */
    val newArgs = mapProductIterator(recursiveTransform)

    if (changed) makeCopy(newArgs).asInstanceOf[this.type] else this
  }

  /**
   * Runs [[transformUp]] with `rule` on all expressions present in this query operator.
   *
   * 自底向上对该操作中的所有表达式进行规则应用。
   *
   * @param rule the rule to be applied to every expression in this operator.
   * @return
   */
  def transformExpressionsUp(rule: PartialFunction[Expression, Expression]): this.type = {
    var changed = false

    @inline def transformExpressionUp(e: Expression): Expression = {
      // 用后序遍历的方式将规则作用于所有的节点
      val newE = e.transformUp(rule)
      if (newE.fastEquals(e)) {
        e
      } else {
        changed = true
        newE
      }
    }

    def recursiveTransform(arg: Any): AnyRef = arg match {
      case e: Expression => transformExpressionUp(e)
      case Some(e: Expression) => Some(transformExpressionUp(e))
      case m: Map[_, _] => m
      case d: DataType => d // Avoid unpacking Structs
      case seq: Traversable[_] => seq.map(recursiveTransform)
      case other: AnyRef => other
      case null => null
    }

    val newArgs = mapProductIterator(recursiveTransform)

    if (changed) makeCopy(newArgs).asInstanceOf[this.type] else this
  }

  /**
   * Returns the result of running [[transformExpressions]] on this node
   * and all its children.
   *
   * 返回对本节点及其所有子节点调用transformExpressions方法应用规则后的结果。
   */
  def transformAllExpressions(rule: PartialFunction[Expression, Expression]): this.type = {
    transform {
      case q: QueryPlan[_] => q.transformExpressions(rule).asInstanceOf[PlanType]
    }.asInstanceOf[this.type]
  }

  /**
   * Returns all of the expressions present in this query plan operator.
   *
   * 得到该节点中的所有表达式列表
   **/
  final def expressions: Seq[Expression] = {
    // Recursively find all expressions from a traversable.
    def seqToExpressions(seq: Traversable[Any]): Traversable[Expression] = seq.flatMap {
      case e: Expression => e :: Nil
      case s: Traversable[_] => seqToExpressions(s)
      case other => Nil
    }

    productIterator.flatMap {
      case e: Expression => e :: Nil
      case Some(e: Expression) => e :: Nil
      case seq: Traversable[_] => seqToExpressions(seq)
      case other => Nil
    }.toSeq
  }

  // 对应output输出属性的schema信息
  lazy val schema: StructType = StructType.fromAttributes(output)

  /** Returns the output schema in the tree format. */
  def schemaString: String = schema.treeString

  /** Prints out the schema in the tree format */
  // scalastyle:off println
  def printSchema(): Unit = println(schemaString)
  // scalastyle:on println

  /**
   * A prefix string used when printing the plan.
   *
   * We use "!" to indicate an invalid plan, and "'" to indicate an unresolved plan.
   *
   * 在QueryPlan的默认实现中：
   * - 如果该计划不可用（invalid），则前缀会用感叹号（“！”）标记。
   * - 如果该计划未解析（unresolved），则前缀会用单引号（“'”）标记。
   */
  protected def statePrefix = if (missingInput.nonEmpty && children.nonEmpty) "!" else ""

  override def simpleString: String = statePrefix + super.simpleString

  override def verboseString: String = simpleString

  /**
   * All the subqueries of current plan.
   *
   * 默认实现该QueryPlan节点中包含的所有子查询
   */
  def subqueries: Seq[PlanType] = {
    expressions.flatMap(_.collect {
      case e: PlanExpression[_] => e.plan.asInstanceOf[PlanType]
    })
  }

  // 默认实现该QueryPlan节点中包含的所有子查询
  override protected def innerChildren: Seq[QueryPlan[_]] = subqueries

  /**
   * Canonicalized copy of this query plan.
   *
   * 直接赋值为当前的QueryPlan类
   */
  protected lazy val canonicalized: PlanType = this

  /**
   * Returns true when the given query plan will return the same results as this query plan.
   *
   * Since its likely undecidable to generally determine if two given plans will produce the same
   * results, it is okay for this function to return false, even if the results are actually
   * the same.  Such behavior will not affect correctness, only the application of performance
   * enhancements like caching.  However, it is not acceptable to return true if the results could
   * possibly be different.
   *
   * By default this function performs a modified version of equality that is tolerant of cosmetic
   * differences like attribute naming and or expression id differences. Operators that
   * can do better should override this function.
   *
   * 利用canonicalized来判断两个QueryPlan的输出数据是否相同
   */
  def sameResult(plan: PlanType): Boolean = {
    val left = this.canonicalized
    val right = plan.canonicalized
    left.getClass == right.getClass &&
      left.children.size == right.children.size &&
      left.cleanArgs == right.cleanArgs &&
      (left.children, right.children).zipped.forall(_ sameResult _)
  }

  /**
   * All the attributes that are used for this plan.
   *
   * 节点所涉及的所有属性（Attribute）列表
   */
  lazy val allAttributes: AttributeSeq = children.flatMap(_.output)

  protected def cleanExpression(e: Expression): Expression = e match {
    case a: Alias =>
      // As the root of the expression, Alias will always take an arbitrary exprId, we need
      // to erase that for equality testing.
      val cleanedExprId =
        Alias(a.child, a.name)(ExprId(-1), a.qualifier, isGenerated = a.isGenerated)
      BindReferences.bindReference(cleanedExprId, allAttributes, allowFailures = true)
    case other =>
      BindReferences.bindReference(other, allAttributes, allowFailures = true)
  }

  /** Args that have cleaned such that differences in expression id should not affect equality */
  protected lazy val cleanArgs: Seq[Any] = {
    def cleanArg(arg: Any): Any = arg match {
      // Children are checked using sameResult above.
      case tn: TreeNode[_] if containsChild(tn) => null
      case e: Expression => cleanExpression(e).canonicalized
      case other => other
    }

    // 对所有构造参数进行map操作
    mapProductIterator {
      case s: Option[_] => s.map(cleanArg) // 直接clean
      case s: Seq[_] => s.map(cleanArg) // 对每个元素clean
      case m: Map[_, _] => m.mapValues(cleanArg) // 对每个键值对的值clean
      case other => cleanArg(other) // 直接clean
    }.toSeq
  }
}
