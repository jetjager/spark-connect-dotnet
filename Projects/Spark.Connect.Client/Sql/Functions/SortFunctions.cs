

using Spark.Connect;
using Spark.Connect.Client.Helpers;
using Spark.Connect.Client.Sql;
using static Spark.Connect.Expression;


namespace Spark.Connect.Client.Sql;

public static partial class Functions
{
    // Sort functions

    /// <summary>
    /// Returns a sort expression based on ascending order of the column.
    /// Example: df.Sort(Asc("dept"), Desc("age"))
    /// </summary>
    public static Column Asc(string columnName) => new Column(columnName).Asc();

    /// <summary>
    /// Returns a sort expression based on ascending order of the column, and null values return
    /// before non-null values.
    /// Example: df.Sort(AscNullsFirst("dept"), Desc("age"))
    /// </summary>
    public static Column AscNullsFirst(string columnName) => new Column(columnName).AscNullsFirst();

    /// <summary>
    /// Returns a sort expression based on ascending order of the column, and null values appear
    /// after non-null values.
    /// Example: df.Sort(AscNullsLast("dept"), Desc("age"))
    /// </summary>
    public static Column AscNullsLast(string columnName) => new Column(columnName).AscNullsLast();

    /// <summary>
    /// Returns a sort expression based on the descending order of the column.
    /// Example: df.Sort(Asc("dept"), Desc("age"))
    /// </summary>
    public static Column Desc(string columnName) => new Column(columnName).Desc();

    /// <summary>
    /// Returns a sort expression based on the descending order of the column, and null values appear
    /// before non-null values.
    /// Example: df.Sort(Asc("dept"), DescNullsFirst("age"))
    /// </summary>
    public static Column DescNullsFirst(string columnName) => new Column(columnName).DescNullsFirst();

    /// <summary>
    /// Returns a sort expression based on the descending order of the column, and null values appear
    /// after non-null values.
    /// Example: df.Sort(Asc("dept"), DescNullsLast("age"))
    /// </summary>
    public static Column DescNullsLast(string columnName) => new Column(columnName).DescNullsLast();


}