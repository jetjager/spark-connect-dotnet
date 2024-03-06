# Spark Connect for .NET
This is a proof of concept that `C#` can call Spark via the new Connect API.
I hope to build on the repository and implement the same Spark API as we have in `Scala`.

## Quick start
- Follow the guide on https://spark.apache.org/docs/latest/spark-connect-overview.html#download-and-start-spark-server-with-spark-connect to start a Spark server with Spark Connect.
- In this case the Spark Connect service is at http://localhost:15002
- In the root of the repository run `dotnet run --project Projects/Spark.Connect.Examples` to run the example below:

```C#
using Spark.Connect.Client;
// Connect to Spark Connect via the default port
var session = new SparkConnectClient("http://localhost:15002").CreateSession();

var version = session.Version;
Console.WriteLine("SparkVersion is: " + version);
/* Outputs
SparkVersion is: { "version": "3.5.0" } 
*/

var df = session.Sql(
@"
select 'Scala' as Language, 2001 as Year
  union
select 'Python' as Language, 1991 as Year
  union 
select 'R' as Language, 1993 as Year
  union
select 'C#' as Language, 2000 as Year
");


df.OrderBy("Year").ShowString(20, false);
/* Outputs
+--------+----+
|Language|Year|
+--------+----+
|Scala   |2001|
|C#      |2000|
|R       |1993|
|Python  |1991|
+--------+----+
*/


df.Filter("Year >= 2000").ShowString(20, false);
/* Outputs
+--------+----+
|Language|Year|
+--------+----+
|Scala   |2001|
|C#      |2000|
+--------+----+
*/

df.SelectExpr("lower(Language) as Language", "Year - 2000 as YearsAfter2000").ShowString(20, false);
/* Outputs
+--------+--------------+
|Language|YearsAfter2000|
+--------+--------------+
|scala   |1             |
|python  |-9            |
|r       |-7            |
|c#      |0             |
+--------+--------------+
*/

df.SelectExpr("*", "Year - 2000 as YearsAfter2000")
  .GroupBy("YearsAfter2000")
  .Agg(("Year", "average"))
  .ShowString(20, false);
/* Outputs
+--------------+---------+
|YearsAfter2000|avg(Year)|
+--------------+---------+
|1             |2001.0   |
|-9            |1991.0   |
|-7            |1993.0   |
|0             |2000.0   |
+--------------+---------+
*/
```
