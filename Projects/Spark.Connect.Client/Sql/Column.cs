
using Spark.Connect;
using static Spark.Connect.Expression.Types;
using static Spark.Connect.Client.Sql.Functions;
using static Spark.Connect.Expression.Types.SortOrder.Types;
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

    // Sort functions

    /// <summary>
    /// Returns a sort expression based on the descending order of the column.
    /// Usage: df.Sort(df["age"].Desc)
    /// </summary>
    public Column Desc() => DescNullsLast();

    /// <summary>
    /// Returns a sort expression based on the descending order of the column, and null values appear
    /// before non-null values.
    /// Usage: df.Sort(df["age"].DescNullsFirst)
    /// </summary>
    public Column DescNullsFirst() => BuildSortOrder(SortDirection.Descending, NullOrdering.SortNullsFirst);

    /// <summary>
    /// Returns a sort expression based on the descending order of the column, and null values appear
    /// after non-null values.
    /// Usage: df.Sort(df["age"].DescNullsLast)
    /// </summary>
    public Column DescNullsLast() => BuildSortOrder(SortDirection.Descending, NullOrdering.SortNullsLast);

    /// <summary>
    /// Returns a sort expression based on ascending order of the column.
    /// Usage: df.Sort(df["age"].Asc)
    /// </summary>
    public Column Asc() => AscNullsFirst();

    /// <summary>
    /// Returns a sort expression based on ascending order of the column, and null values return
    /// before non-null values.
    /// Usage: df.Sort(df["age"].AscNullsFirst)
    /// </summary>
    public Column AscNullsFirst() => BuildSortOrder(SortDirection.Ascending, NullOrdering.SortNullsFirst);

    /// <summary>
    /// Returns a sort expression based on ascending order of the column, and null values appear
    /// after non-null values.
    /// Usage: df.Sort(df["age"].AscNullsLast)
    /// </summary>
    public Column AscNullsLast() => BuildSortOrder(SortDirection.Ascending, NullOrdering.SortNullsLast);

    private Column BuildSortOrder(SortDirection sortDirection, NullOrdering nullOrdering)
    {

        var sortedExpr  = new Expression
        {
            SortOrder = new SortOrder
            {
                Child = expr,
                Direction = sortDirection,
                NullOrdering = nullOrdering,
            }
        };

        return new Column(sortedExpr);
    }




    public Column IsNaN() => Fn("isNaN");

}

