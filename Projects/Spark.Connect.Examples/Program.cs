
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
|Python  |1991|
|R       |1993|
|C#      |2000|
|Scala   |2001|
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