using Spark.Connect.Client.Sql;

namespace Spark.Connect.Client.Tests;

public class DataFrameTests
{
    SparkSession _session;
    public DataFrameTests()
    {
        _session = new SparkConnectClient("http://localhost:15002").CreateSession();
    }




    [Fact]
    public void SelectExpr()
    {
        // Arrange
        var df = _session.Sql("select 'Scala' as Language, 2001 as Year");
        var expected = _session.Sql("select 'scala' as Language, 1 as YearsAfter2000");

        // Act
        var actualFrame = df.SelectExpr("lower(Language) as Language", "Year - 2000 as YearsAfter2000");


        // Assert
        Assert.Equal(expected, actualFrame);

    }
}