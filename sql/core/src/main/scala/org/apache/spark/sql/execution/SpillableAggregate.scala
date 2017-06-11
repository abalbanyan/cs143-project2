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

import org.apache.spark.sql.catalyst.errors._
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans.physical.{ClusteredDistribution, AllTuples, UnspecifiedDistribution}
import org.apache.spark.util.collection.SizeTrackingAppendOnlyMap

case class SpillableAggregate(
                               partial: Boolean,
                               groupingExpressions: Seq[Expression],
                               aggregateExpressions: Seq[NamedExpression],
                               child: SparkPlan) extends UnaryNode {

  override def requiredChildDistribution =
    if (partial) {
      UnspecifiedDistribution :: Nil
    } else {
      if (groupingExpressions == Nil) {
        AllTuples :: Nil
      } else {
        ClusteredDistribution(groupingExpressions) :: Nil
      }
    }

  override def output = aggregateExpressions.map(_.toAttribute)

  /**
    * An aggregate that needs to be computed for each row in a group.
    *
    * @param unbound Unbound version of this aggregate, used for result substitution.
    * @param aggregate A bound copy of this aggregate used to create a new aggregation buffer.
    * @param resultAttribute An attribute used to refer to the result of this aggregate in the final
    *                        output.
    */
  case class ComputedAggregate(
                                unbound: AggregateExpression,
                                aggregate: AggregateExpression,
                                resultAttribute: AttributeReference)

  /** Physical aggregator generated from a logical expression.  */
  private[this] val aggregator: ComputedAggregate =
  {
    val x = aggregateExpressions.flatMap { agg =>
      agg.collect {
        case a: AggregateExpression =>
          ComputedAggregate(
            a,
            BindReferences.bindReference(a, child.output),
            AttributeReference(s"aggResult:$a", a.dataType, a.nullable)())
      }
    }.toArray

    x(0)
  }

  /** Schema of the aggregate.  */
  private[this] val aggregatorSchema: AttributeReference = aggregator.resultAttribute // Implemented?

  /** Creates a new aggregator instance.  */
  private[this] def newAggregatorInstance(): AggregateFunction = aggregator.aggregate.newInstance() // Implemented?

  /** Named attributes used to substitute grouping attributes in the final result. */
  private[this] val namedGroups = groupingExpressions.map {
    case ne: NamedExpression => ne -> ne.toAttribute
    case e => e -> Alias(e, s"groupingExpr:$e")().toAttribute
  }

  /**
    * A map of substitutions that are used to insert the aggregate expressions and grouping
    * expression into the final result expression.
    */
  protected val resultMap =
  ( Seq(aggregator.unbound -> aggregator.resultAttribute) ++ namedGroups).toMap

  /**
    * Substituted version of aggregateExpressions expressions which are used to compute final
    * output rows given a group and the result of all aggregate computations.
    */
  private[this] val resultExpression = aggregateExpressions.map(agg => agg.transform {
    case e: Expression if resultMap.contains(e) => resultMap(e)
  }
  )

  override def execute() = attachTree(this, "execute") {
    child.execute().mapPartitions(iter => generateIterator(iter))
  }

  /**
    * This method takes an iterator as an input. The iterator is drained either by aggregating
    * values or by spilling to disk. Spilled partitions are successively aggregated one by one
    * until no data is left.
    *
    * @param input the input iterator
    * @param memorySize the memory size limit for this aggregate
    * @return the result of applying the projection
    */
  def generateIterator(input: Iterator[Row], memorySize: Long = 64 * 1024 * 1024, numPartitions: Int = 64): Iterator[Row] = {
    val groupingProjection = CS143Utils.getNewProjection(groupingExpressions, child.output)
    var currentAggregationTable = new SizeTrackingAppendOnlyMap[Row, AggregateFunction]
    var data = input

    // Iterate through all the rows via input and add them to currentAggregationTable which maps each row to an AggregateFunction.
    // We put each row into the hash table until it has the potential to spill.
    // From there, spill if the group that the newly added row belongs to does not already exist in the hash table.

    // Within initSpills(), you should create a new DiskHashedRelation comprising of empty partitions, that are filled up later by
    // spillRecord() from within aggregate().

    // The aggregate functions themselves will be evaluated in the AggregateIteratorGenerator.
    // Aggregate values:  The input data.
    // Aggregate results: Result obtained after computing the corresponding aggregate function over the input data (aggregate values).
    // It needs to return two rows: input row + aggregate result (input row is grouping).

    // Logic of generateIterator:
    // 1) Drain the input iterator into the aggregation table
    // 2) generate an aggregate iterator using the helper function AggregateIteratorGenerator properly formatting the aggregate result
    // 3) use the iterator inside generateIterator as an external inteface to access and drive the aggregate iterator.


    // Within initSpills() you should create a new DiskHashedRelation comprising of empty partitions
    // These are filled up later by spillRecord.
    def initSpills(): Array[DiskPartition]  = {
      // IMPLEMENTED?

      val partitions = new Array[DiskPartition](numPartitions)
      for(i <- 0 until numPartitions){
        partitions(i) = new DiskPartition("spill_partition_" + i.toString, 0)
      }
      partitions
    }

    val spills = initSpills()

    new Iterator[Row] {
      /**
        * Global object wrapping the spills into a DiskHashedRelation. This variable is
        * set only when we are sure that the input iterator has been completely drained.
        *
        * @return
        */
      var hashedSpills: Option[Iterator[DiskPartition]] = None
      var aggregateResult: Iterator[Row] = aggregate() // The hash table iterator.

      def hasNext() = {
         aggregateResult.hasNext || fetchSpill() // ?
      }

      def next() = {
        if(aggregateResult.hasNext) aggregateResult.next() else null
      }

      /**
        * This method load the aggregation hash table by draining the data iterator
        *
        * @return
        */
      private def aggregate(): Iterator[Row] = {
        /* IMPLEMENT THIS METHOD */
        // Plan: iterate through all of data, and fill the hash table.
        // This method will call AggregateIteratorGenerator.
        // AggregateIteratorGeneral accepts: resultExpressions,inputSchema
        // It will return a function that converts the currentAggregationTable Iterator[(Row,AggregateFunction)] to an Iterator[Row].
        // We return the result of this function.

        // Fill Hash Table?
        if(groupingExpressions.isEmpty){
          val instance = newAggregatorInstance()
          var currentRow : Row = null
          while(data.hasNext){
            currentRow = data.next()
            instance.update(currentRow)
          }

          val resultProjection = new InterpretedProjection(resultExpression, Seq(aggregatorSchema))
          val aggregateResults = new GenericMutableRow(1)
          aggregateResults(0) = instance.eval(EmptyRow)

//          // Task 7:
//          hashedSpills = Some {spills.iterator}

          Iterator(resultProjection(aggregateResults))
        } else {
          while (data.hasNext) {
            val currentRow = data.next()
            val currentGroup = groupingProjection(currentRow)
            var currentInstance = currentAggregationTable(currentGroup)
            if (currentInstance == null) {
              currentInstance = newAggregatorInstance()

              // Task 6:
              if(CS143Utils.maybeSpill(currentAggregationTable, memorySize)){
                spillRecord(currentRow)
              } else {
                currentAggregationTable.update(currentGroup.copy(), currentInstance)
              }
              //


            }
            currentInstance.update(currentRow)
          }
          val aggregate_iterator_gen = AggregateIteratorGenerator(resultExpression, Seq(aggregatorSchema) ++ namedGroups.map(_._2)) // idk what the input should be
          val aggregate_iterator = aggregate_iterator_gen(currentAggregationTable.iterator)

          // Task 7:
          for(partition <- spills){
            partition.closeInput()
          }
          hashedSpills = Some {spills.iterator}

          aggregate_iterator
        }

      }

      /**
        * Spill input rows to the proper partition using hashing
        *
        * @return
        */
      private def spillRecord(row: Row)  = {
        /* IMPLEMENT THIS METHOD */
        spills(row.hashCode() % numPartitions).insert(row)   // Not sure what to project row with.
        // spills(0).insert(row)   // Not sure what to project row with.
      }



      /**
        * This method fetches the next records to aggregate from spilled partitions or returns false if we
        * have no spills left.
        *
        * @return
        */
      private def fetchSpill(): Boolean  = {
        /* IMPLEMENT THIS METHOD */
        // Check spills. Put the entire partition into currentAggregateTable.
        hashedSpills match {
          case Some(spill) => { // spill is an iterator through the array of partitions
            if(spill.hasNext){
              val partition = spill.next()
              currentAggregationTable = new SizeTrackingAppendOnlyMap[Row, AggregateFunction]
              // fill table with partition
              val partition_iterator = partition.getData()
              if(partition_iterator.hasNext) {
                var currentRow = partition_iterator.next()
                val currentGroup = groupingProjection(currentRow)
                var currentInstance = currentAggregationTable(currentGroup)
                if (currentInstance == null) {
                  currentInstance = newAggregatorInstance()
                  currentAggregationTable.update(currentGroup.copy(), currentInstance)
                }
                currentInstance.update(currentRow)

                while (partition_iterator.hasNext) {
                  var currentRow = partition_iterator.next()
                  val currentGroup = groupingProjection(currentRow)
                  var currentInstance = currentAggregationTable(currentGroup)
                  if (currentInstance == null) {
                    currentInstance = newAggregatorInstance()
                    currentAggregationTable.update(currentGroup.copy(), currentInstance)
                  }
                  currentInstance.update(currentRow)
                }

                // Reset the hash table iterator.
                val aggregate_iterator_gen = AggregateIteratorGenerator(resultExpression, Seq(aggregatorSchema) ++ namedGroups.map(_._2)) // idk what the input should be
                aggregateResult = aggregate_iterator_gen(currentAggregationTable.iterator)
                true

              } else {
                fetchSpill()
              }

            } else {
              hashedSpills = None
              false
            }
          }
          case None => {false}
        }
      }
    }
  }
}
