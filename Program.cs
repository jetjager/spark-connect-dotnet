
//dotnet-grpc add-url --service Client https://github.com/apache/spark/tree/master/connector/connect/common/src/main/protobuf

// See https://aka.ms/new-console-template for more information
using Grpc.Net.Client;
using Spark.Connect;

// Connect to Spark Connect via the default port
using var channel = GrpcChannel.ForAddress("http://localhost:15002");
var client = new SparkConnectService.SparkConnectServiceClient(channel);
var userContext = new UserContext();
var session = new SparkSession(client, userContext);

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


df.OrderBy("Year").ShowString(20, false);
df.Filter("Year >= 2000").ShowString(20, false);

df.SelectExpr("lower(Language) as Language", "Year - 2000 as YearsAfter2000").ShowString(20, false);

df.SelectExpr("*", "Year - 2000 as YearsAfter2000")
  .GroupBy("YearsAfter2000")
  .Agg(("Year", "average"))
  .ShowString(20, false);