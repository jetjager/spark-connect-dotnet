
using Spark.Connect;
using Spark.Connect.Client.Helpers;
using Spark.Connect.Client.Sql;
using static Spark.Connect.Expression;


namespace Spark.Connect.Client.Sql;

public static class Functions
{
    /// <summary>
    /// Returns a Column based on the given column name.
    /// </summary>
    /// <param name="colName">The name of the column.</param>
    /// <returns>A new Column instance.</returns>
    public static Column Col(string colName)
    {
        return new Column(colName);
    }



    /// <summary>
    /// Creates a Column of literal value.
    ///
    /// The passed in object is returned directly if it is already a Column. If the object is a
    /// string representing a symbol, it is converted into a Column also. Otherwise, a new Column is created
    /// to represent the literal value.
    /// </summary>
    /// <param name="literal">The literal value to convert into a Column.</param>
    /// <returns>A Column representing the literal value.</returns>
    public static Column Lit(object literal)
    {
        switch (literal)
        {
            case Column c:
                return c;
            default:
                return CreateLiteral(literal);
        }
    }

    private static Column CreateLiteral(object literal)
    {
        var newExpr = new Expression
        {
            Literal = Conversions.ConvertObjectToLiteral(literal)
        };

        return new Column(newExpr);
    }

    public static Column Lit(string symbol)
    {
        return new Column(symbol);
    }

}