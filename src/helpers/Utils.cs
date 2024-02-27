
using Grpc.Core;
using Spark.Connect;

class Utils {
        public static async IAsyncEnumerable<ExecutePlanResponse> ReadExecutedPlan(AsyncServerStreamingCall<ExecutePlanResponse> call)
    {
        await foreach (var response in call.ResponseStream.ReadAllAsync())
        {
            yield return response;
        }
    }
}