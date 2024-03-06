using Spark.Connect;

namespace Spark.Connect.Client.Sql;
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
    /// Returns a new Dataset sorted by the given expressions. This is an alias of the `sort` function.
    /// </summary>
    /// <param name="sortCols"></param>
    /// <returns></returns>
    public DataFrame OrderBy(params string[] sortCols)
    {
        var orders = sortCols.Select(expr => new Expression.Types.SortOrder
        {
            Child = new Expression
            {
                ExpressionString = new Expression.Types.ExpressionString
                {
                    Expression = expr
                }
            }
        });
        var newRelation = new Relation()
        {
            Sort = new Sort
            {
                Input = _relation,
                Order = { orders }
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