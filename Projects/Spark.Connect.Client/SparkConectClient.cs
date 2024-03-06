using Grpc.Net.Client;
using Spark.Connect;
using Spark.Connect.Client.Sql;
using Spark.Connect.Client.Helpers;

namespace Spark.Connect.Client;

public class SparkConnectClient
{
    private GrpcChannel _channel;

    // Constructor to initialize the GrpcChannel
    public SparkConnectClient(string endpoint)
    {
        _channel = GrpcChannel.ForAddress(endpoint);
    }

    // Method to create a SparkSession
    public SparkSession CreateSession()
    {
        var client = new SparkConnectService.SparkConnectServiceClient(_channel);
        var userContext = new UserContext();
        var session = new SparkSession(client, userContext);
        return session;
    }

    public void Dispose()
    {
        if (_channel != null)
        {
            _channel.Dispose();
            _channel = null;
        }
    }
}

