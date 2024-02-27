using System.Globalization;
using Spark.Connect;
using static Spark.Connect.Aggregate.Types;

public class RelationalGroupedDataset
{

    private readonly SparkSession _sparkSession;
    private readonly Relation _relation;
    private readonly Expression[] _groupingExprs;

    private readonly GroupType _groupType;
    public RelationalGroupedDataset(SparkSession sparkSession, Relation relation, Expression[] groupingExprs, GroupType groupType)
    {
        _sparkSession = sparkSession ?? throw new ArgumentNullException(nameof(sparkSession));
        _relation = relation ?? throw new ArgumentNullException(nameof(relation));
        _groupingExprs = groupingExprs ?? throw new ArgumentNullException(nameof(groupingExprs));
        _groupType = groupType;
    }

    public DataFrame Agg(params (string, string)[] aggExprs)
    {

        var aggregateExpression = aggExprs.Select(expr => strToExpr(expr.Item2, new Expression
        {
            ExpressionString = new Expression.Types.ExpressionString
            {
                Expression = expr.Item1
            }
        }));


        var newRelation = new Relation()
        {
            Aggregate = new Aggregate
            {
                Input = _relation,
                AggregateExpressions = { aggregateExpression },
                GroupingExpressions = { _groupingExprs },
                GroupType = _groupType
            }
        };

        return new DataFrame(_sparkSession, newRelation);
    }

    private Expression strToExpr(string expression, Expression inputExpressionString)
    {
        switch (expression.ToLower(CultureInfo.InvariantCulture))
        {
            case "avg":
            case "average":
            case "mean":
                return new Expression
                {
                    UnresolvedFunction = new Expression.Types.UnresolvedFunction
                    {
                        FunctionName = "avg",
                        Arguments = { inputExpressionString },
                        IsDistinct = false
                    }
                };
            case "stddev":
            case "std":
                return new Expression
                {
                    UnresolvedFunction = new Expression.Types.UnresolvedFunction
                    {
                        FunctionName = "stddev",
                        Arguments = { inputExpressionString },
                        IsDistinct = false
                    }
                };

            case "count":
            case "size":
                return new Expression
                {
                    UnresolvedFunction = new Expression.Types.UnresolvedFunction
                    {
                        FunctionName = "avg",
                        Arguments = { inputExpressionString },
                        IsDistinct = false
                    }
                };
            default:
                return new Expression
                {
                    UnresolvedFunction = new Expression.Types.UnresolvedFunction
                    {
                        FunctionName = expression,
                        Arguments = { inputExpressionString },
                        IsDistinct = false
                    }
                };
        }
    }
}