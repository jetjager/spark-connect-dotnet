
using Spark.Connect;
using static Spark.Connect.Expression.Types;
using static Spark.Connect.Client.Sql.Functions;
namespace Spark.Connect.Client.Sql;

public class Column(Expression expr)
{
    internal Expression expr = expr;

    public Column(string name) : this(NameToExpression(name))
    {

    }

    private static Expression NameToExpression(string name)
    {
        return name switch
        {
            "*" => new Expression()
            {
                UnresolvedStar = new UnresolvedStar()
            },
            string s when s.EndsWith(".*") => new Expression()
            {
                UnresolvedStar = new UnresolvedStar()
                {
                    UnparsedTarget = name
                }
            },
            _ => new Expression()
            {
                UnresolvedAttribute = new UnresolvedAttribute()
                {
                    UnparsedIdentifier = name
                }
            },
        };
    }

    private Column Fn(string name) => Fn(name, false, this);
    private Column Fn(string name, Column other) => Fn(name, false, other);
    private Column Fn(string name, object other) => Fn(name, false, Lit(other));
    private Column Fn(string name, bool isDistinct, params Column[] inputs)
    {
        var newExpr = new Expression
        {
            UnresolvedFunction = new UnresolvedFunction
            {
                FunctionName = name,
                Arguments = { inputs.Select(c => c.expr) },
                IsDistinct = isDistinct
            }
        };


        return new Column(newExpr);
    }

    public Column IsNaN() => Fn("isNaN");

}

