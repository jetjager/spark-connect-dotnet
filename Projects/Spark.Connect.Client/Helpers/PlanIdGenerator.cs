namespace Spark.Connect.Client.Helpers;

public class PlanIdGenerator
{
    private static long atomicInt64 = 0;

    public static long NewPlanId()
    {
        // Increment the value atomically and return the new value
        return Interlocked.Increment(ref atomicInt64);
    }
}
