
using Spark.Connect.Client;

// Connect to Spark Connect via the default port
var session = new SparkConnectClient("http://localhost:15002").CreateSession();

var version = session.Version;
Console.WriteLine("SparkVersion is: " + version);

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


var rows = df.Collect();
Console.WriteLine("Number of Rows: " + rows.Count);
rows.ForEach(Console.WriteLine);