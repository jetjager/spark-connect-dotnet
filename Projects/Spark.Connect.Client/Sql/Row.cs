
namespace Spark.Connect.Client.Sql;

public interface IRow
{
    StructType Schema();
    object[] Values();
}

public class GenericRowWithSchema : IRow
{
    private object[] _values;
    private StructType _schema;

    public GenericRowWithSchema(object[] values, StructType schema)
    {
        _values = values;
        _schema = schema;
    }

    public StructType Schema()
    {
        return _schema;
    }

    public object[] Values()
    {
        return _values;
    }

    override public string ToString()
    {
        return string.Join(", ", Values()); // TODO: what is a good string representation?
    }
}
