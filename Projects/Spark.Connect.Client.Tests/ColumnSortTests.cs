using System.Diagnostics.CodeAnalysis;
using Spark.Connect.Client.Sql;
using static Spark.Connect.Client.Sql.Functions;
namespace Spark.Connect.Client.Tests;

public class ColumnOperationsTests : SparkIntegrationTest
{

    [Fact]
    public void OrderBy_SortsAscNullFirst_AsDefault()
    {
        // Act
        var actual = _dataFrameTesla.Select(Col("Year")).OrderBy(Col("Year"));

        // Assert
        EqualRowsContents([[null], [2012], [2015], [2017], [2020]], actual);
    }

    [Fact]
    public void OrderByDescFunction_SortsDescNullFirst_AsDefault()
    {
        // Act
        var actual = _dataFrameTesla.Select(Col("Year")).OrderBy(Desc("Year"));

        // Assert
        EqualRowsContents([[2020], [2017], [2015], [2012], [null]], actual);
    }

}