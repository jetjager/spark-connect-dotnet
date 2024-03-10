using Spark.Connect;

namespace Spark.Connect.Client.Sql;

using System.ComponentModel.DataAnnotations;
using Spark.Connect.Client.Helpers;
public class DataFrame
{
    private readonly SparkSession _sparkSession;
    private readonly Relation _relation;
    public DataFrame(SparkSession sparkSession, Relation relation)
    {
        _sparkSession = sparkSession ?? throw new ArgumentNullException(nameof(sparkSession));
        _relation = relation ?? throw new ArgumentNullException(nameof(relation));
    }

    public void ShowString(int numRows, bool truncate)
    {
        var vertical = false;
        var truncateValue = truncate ? 20 : 0;

        var plan = new Plan
        {
            Root = new Relation
            {
                Common = new RelationCommon
                {
                    PlanId = PlanIdGenerator.NewPlanId()
                },
                ShowString = new ShowString
                {
                    Input = _relation,
                    NumRows = numRows,
                    Truncate = truncateValue,
                    Vertical = vertical
                }
            }

        };


        var responseClient = _sparkSession.ExecutePlan(plan);
        var all = Utils.ReadExecutedPlan(responseClient)
                         .Where(x => x.ResponseTypeCase == ExecutePlanResponse.ResponseTypeOneofCase.ArrowBatch)
                         .Select(response => ArrowFunctions.ReadArrowBatchData(response.ArrowBatch.Data.ToByteArray(), null));

        // we only expect Spark to return a single column and single row
        all.ToBlockingEnumerable().ToList().ForEach(row => row.ForEach(r => Console.WriteLine(r.Values()[0])));

    }

    public List<IRow> Collect()
    {
        var newRelation = _relation.Clone();
        newRelation.Common = new RelationCommon
        {
            PlanId = PlanIdGenerator.NewPlanId()
        };

        var plan = new Plan
        {
            Root = new Relation(newRelation)
        };

        var responseClient = _sparkSession.ExecutePlan(plan);
        var all = Utils.ReadExecutedPlan(responseClient).ToBlockingEnumerable().ToList();
        var responseSchema = all.First(x => x.Schema != null).Schema;

        var fields = Conversions.ConvertProtoStructFields(responseSchema.Struct.Fields.ToList());
        var schema = new StructType("", fields); // todo what is the typename is this case?

        var rows = all.Where(x => x.ResponseTypeCase == ExecutePlanResponse.ResponseTypeOneofCase.ArrowBatch)
                         .SelectMany(response => ArrowFunctions.ReadArrowBatchData(response.ArrowBatch.Data.ToByteArray(), schema)).ToList();

        return rows;
    }

    /// <summary>
    /// Filters rows using the given condition.
    /// </summary>
    /// <remarks>
    /// <para>The following are equivalent:</para>
    /// <code>
    /// peopleDs.Filter($"age" > 15)
    /// peopleDs.Where($"age" > 15)
    /// </code>
    /// This method allows for concise and readable querying expressions.
    /// </remarks>
    public DataFrame Filter(string condExpr)
    {
        var newRelation = new Relation()
        {
            Filter = new Filter
            {
                Input = _relation,
                Condition = new Expression
                {
                    ExpressionString = new Expression.Types.ExpressionString
                    {
                        Expression = condExpr
                    }
                }
            }
        };

        return new DataFrame(_sparkSession, newRelation);
    }

    public DataFrame Filter(Column condition)
    {
        var newRelation = new Relation()
        {
            Filter = new Filter
            {
                Input = _relation,
                Condition = condition.expr
            }
        };

        return new DataFrame(_sparkSession, newRelation);
    }

    /// <summary>
    /// Selects a set of SQL expressions. This is a variant of <c>Select</c> that accepts
    /// SQL expressions.
    /// </summary>
    /// <remarks>
    /// <para>The following are equivalent:</para>
    /// <code>
    /// ds.SelectExpr("colA", "colB as newName", "abs(colC)")
    /// ds.Select(new Expr("colA"), new Expr("colB as newName"), new Expr("abs(colC)"))
    /// </code>
    /// </remarks>
    /// <example>
    /// This example shows how to use the <c>selectExpr</c> method:
    /// <code>
    /// ds.SelectExpr("colA", "colB as newName", "abs(colC)")
    /// </code>
    /// </example>
    public DataFrame SelectExpr(params string[] exprs)
    {

        var expressions = exprs.Select(expr => new Expression
        {
            ExpressionString = new Expression.Types.ExpressionString
            {
                Expression = expr
            }
        });
        var newRelation = new Relation()
        {
            Project = new Project
            {
                Input = _relation,
                Expressions = { expressions }
            }
        };

        return new DataFrame(_sparkSession, newRelation);
    }

    /// <summary>
    /// Selects a set of column based expressions.
    /// Example:
    ///   ds.Select(new Column("colA"), new Column("colB").Plus(1))
    /// </summary>
    /// <param name="cols">The columns and expressions to select.</param>
    /// <returns>A new DataFrame with the selected columns and expressions.</returns>
    public DataFrame Select(params Column[] cols)
    {

        var newRelation = new Relation()
        {
            Project = new Project
            {
                Input = _relation,
                Expressions = { cols.Select(col => col.expr) }
            }
        };

        return new DataFrame(_sparkSession, newRelation);
    }


    /// <summary>
    /// Returns a new Dataset sorted by the given expressions. This is an alias of the `sort` function.
    /// </summary>
    /// <param name="sortCols"></param>
    /// <returns></returns>
    public DataFrame OrderBy(params string[] sortCols) => Sort(sortCols);

    // Methods for sorting datasets

    /// <summary>
    /// Returns a new Dataset sorted by the specified column, all in ascending order.
    /// Examples:
    ///   // The following 3 are equivalent
    ///   ds.Sort("sortcol");
    ///   ds.Sort(new Column("sortcol"));
    ///   ds.Sort(new Column("sortcol").Asc());
    /// </summary>
    /// <param name="sortCols">columns to sort by.</param>
    /// <returns>A new Dataset sorted by the specified columns.</returns>
    public DataFrame Sort(params string[] sortCols) => Sort(sortCols.Select(c => new Column(c)).ToArray());

    /// <summary>
    /// Returns a new Dataset sorted by the given expressions. For example:
    ///   ds.Sort(new Column("col1"), new Column("col2").Desc());
    /// </summary>
    /// <param name="sortExprs">The columns and expressions to sort by.</param>
    /// <returns>A new Dataset sorted by the specified expressions.</returns>
    public DataFrame Sort(params Column[] sortExprs)
    {
        // Assume buildSort is a method implemented elsewhere that applies the sorting
        // to the dataset based on the given columns and returns the sorted dataset.
        return BuildSort(global: true, sortExprs);
    }

    /// <summary>
    /// Returns a new Dataset sorted by the given expressions. This is an alias of the Sort function.
    /// </summary>
    /// <param name="sortExprs">The columns and expressions to sort by.</param>
    /// <returns>A new Dataset sorted by the specified expressions.</returns>
    public DataFrame OrderBy(params Column[] sortExprs)
    {
        return Sort(sortExprs);
    }

    private DataFrame BuildSort(bool global, params Column[] sortColumns)
    {

        var orders = sortColumns.Select(col => col.expr.SortOrder ?? col.Asc().expr.SortOrder);

        var newRelation = new Relation()
        {
            Sort = new Sort
            {
                Input = _relation,
                Order = { orders },
                IsGlobal = global
            }
        };

        return new DataFrame(_sparkSession, newRelation);
    }


    public RelationalGroupedDataset GroupBy(params string[] cols)
    {
        var expressions = cols.Select(expr => new Expression
        {
            ExpressionString = new Expression.Types.ExpressionString
            {
                Expression = expr
            }
        }).ToArray();
        return new RelationalGroupedDataset(_sparkSession, _relation, expressions, Aggregate.Types.GroupType.Groupby);

    }

}