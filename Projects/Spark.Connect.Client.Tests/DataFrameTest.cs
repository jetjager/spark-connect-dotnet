using System.Diagnostics.CodeAnalysis;
using Spark.Connect.Client.Sql;
using static Spark.Connect.Client.Sql.Functions;
namespace Spark.Connect.Client.Tests;

public class DataFrameTests : SparkIntegrationTest
{

    [Fact]
    public void Filter_StringCondition()
    {
        // Act
        var actual = _dataFrameProgramingLanguages.Filter("Language='Scala'");

        // Assert
        EqualRowsContents([["Scala", 2001]], actual);
    }

   [Fact]
    public void Filter_ColumnCondition() {
        // Arrange
        var df = _session.Sql("select 3.14 as number union select cast('NaN' as double) as number");

        // Act
        var actual = df.Filter(Col("number").IsNaN());

        EqualRowsContents([[double.NaN]], actual);
    }

    [Fact]
    public void SelectExpr_ProjectsColumns()
    {
        // Arrange
        var df = _session.Sql("select 'Scala' as Language, 2001 as Year");

        // Act
        var actual = df.SelectExpr("lower(Language) as Language", "Year - 2000 as YearsAfter2000");

        // Assert
        EqualRowsContents([["scala", 1]], actual);
    }

    [Fact]
    public void SelectExpr_GroupByColumns()
    {
        // Act
        var actual = _dataFrameTesla
        .GroupBy("VehicleType")
        .Agg(("Year", "average"));

        // Assert
        EqualRowsContents([["SUV", 2017.5], ["Sedan", 2014.5], ["Compact", null]], actual);
    }


}