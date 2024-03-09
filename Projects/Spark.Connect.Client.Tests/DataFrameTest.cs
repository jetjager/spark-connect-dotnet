using System.Diagnostics.CodeAnalysis;
using Spark.Connect.Client.Sql;
using static Spark.Connect.Client.Sql.Functions;
namespace Spark.Connect.Client.Tests;

public class DataFrameTests
{
    SparkSession _session;

    DataFrame _dataFrameProgramingLanguages;
    DataFrame _dataFrameTesla;

    public DataFrameTests()
    {
        _session = new SparkConnectClient("http://localhost:15002").CreateSession();
        _dataFrameProgramingLanguages = _session.Sql(
            @"
            select 'Scala' as Language, 2001 as Year
            union
            select 'Python' as Language, 1991 as Year
            union 
            select 'R' as Language, 1993 as Year
            union
            select 'C#' as Language, 2000 as Year
            ");

        _dataFrameTesla = _session.Sql(
            @"
            select 'Model S' as Model, 2012 as Year, 'Sedan' as VehicleType, 5 as MaxSeats
            union
            select 'Model 3' as Model, 2017 as Year, 'Sedan' as VehicleType, 5 as MaxSeats
            union 
            select 'Model X' as Model, 2015 as Year, 'SUV' as VehicleType, 7 as MaxSeats
            union
            select 'Model Y' as Model, 2020 as Year, 'SUV' as VehicleType, 7 as MaxSeats
            "
        );
    }


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
        EqualRowsContents([["SUV", 2017.5], ["Sedan", 2014.5]], actual);
    }

 

    private void EqualRowsContents(string sql, DataFrame actual)
    {
        var expected = _session.Sql(sql);
        EqualRowsContents(expected, actual);
    }

    private void EqualRowsContents(DataFrame expected, DataFrame actual)
    {
        var expectedRows = expected.Collect().Select(row => row.Values());
        var actualRows = actual.Collect().Select(row => row.Values());

        Assert.Equal(expectedRows, actualRows);
    }


    private void EqualRowsContents(object[][] expected, DataFrame actual)
    {
        var actualRows = actual.Collect().Select(row => row.Values());

        Assert.Equal(expected, actualRows);
    }
}