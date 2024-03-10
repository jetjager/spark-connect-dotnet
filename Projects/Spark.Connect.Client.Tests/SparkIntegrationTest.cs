using Spark.Connect.Client.Sql;
namespace Spark.Connect.Client.Tests;
public abstract class SparkIntegrationTest
{

    protected SparkSession _session;

    protected DataFrame _dataFrameProgramingLanguages;
    protected DataFrame _dataFrameTesla;




    public SparkIntegrationTest()
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
            union
            select 'Model 2' as Model, null as Year, 'Compact' as VehicleType, 5 as MaxSeats
            "
        );
    }

    protected void EqualRowsContents(string sql, DataFrame actual)
    {
        var expected = _session.Sql(sql);
        EqualRowsContents(expected, actual);
    }

    protected void EqualRowsContents(DataFrame expected, DataFrame actual)
    {
        var expectedRows = expected.Collect().Select(row => row.Values());
        var actualRows = actual.Collect().Select(row => row.Values());

        Assert.Equal(expectedRows, actualRows);
    }


    protected void EqualRowsContents(object[][] expected, DataFrame actual)
    {
        var actualRows = actual.Collect().Select(row => row.Values());

        Assert.Equal(expected, actualRows);
    }


}