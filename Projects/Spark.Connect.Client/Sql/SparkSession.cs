using Grpc.Core;
using Spark.Connect;
using static Spark.Connect.SparkConnectService;
using static Spark.Connect.AnalyzePlanRequest;
using Spark.Connect.Client.Helpers;

namespace Spark.Connect.Client.Sql;

// Assuming DeveloperApi and Experimental are custom attributes you'll define or map accordingly
public class SparkSession
{
    private readonly SparkConnectServiceClient _client;
    private readonly string _sessionId;
    private readonly UserContext _userContext;

    public SparkSession(SparkConnectServiceClient client, UserContext userContext)
    {
        _client = client ?? throw new ArgumentNullException(nameof(client));
        _sessionId = "00112233-4455-6677-8899-aabbccddeeff";
        _userContext = userContext ?? throw new ArgumentNullException(nameof(userContext));
    }

    public string SessionId => _sessionId;

    public string Version => LazyInitializer.EnsureInitialized(ref _version, () => GetVersion());

    private string _version;

    private string GetVersion()
    {
        AnalyzePlanRequest request = new AnalyzePlanRequest
        {
            SessionId = "00112233-4455-6677-8899-aabbccddeeff",
            UserContext = _userContext,
            SparkVersion = new Types.SparkVersion()
        };
        var version = _client.AnalyzePlan(request);

        return version.SparkVersion.ToString(); // todo how to write the message and get the value?
    }

    // val conf: RuntimeConfig = new RuntimeConfig(client)

    public AsyncServerStreamingCall<ExecutePlanResponse> ExecutePlan(Plan plan)
    {
        var request = new ExecutePlanRequest
        {
            SessionId = _sessionId,
            UserContext = _userContext,
            Plan = plan
        };

        var responseClient = _client.ExecutePlan(request);

        return responseClient;
    }

    public DataFrame Sql(string query)
    {
        var plan = new Plan
        {
            Command = new Command
            {
                SqlCommand = new SqlCommand
                {
                    Sql = query
                }
            }
        };

        // todo does this return 1 or more dataframes?
        using var call = ExecutePlan(plan);

        var response = Utils.ReadExecutedPlan(call).ToBlockingEnumerable().First();
        var sqlCommandResult = response.SqlCommandResult;

        return new DataFrame(this, sqlCommandResult.Relation);
    }


}
