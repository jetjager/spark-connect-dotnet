
namespace Spark.Connect.Client.Sql;

public interface IDataType
{
    string TypeName();

    public static string GetDataTypeName(IDataType dataType)
    {
        var type = dataType.GetType();
        var name = type.Name;
        if (name.EndsWith("Type"))
        {
            name = name.Substring(0, name.Length - "Type".Length);
        }
        return name;
    }
}

public class BooleanType : IDataType
{
    public string TypeName() => IDataType.GetDataTypeName(this);
}

public class ByteType : IDataType
{
    public string TypeName() => IDataType.GetDataTypeName(this);
}

public class ShortType : IDataType
{
    public string TypeName() => IDataType.GetDataTypeName(this);
}

public class IntegerType : IDataType
{
    public string TypeName() => IDataType.GetDataTypeName(this);
}

public class LongType : IDataType
{
    public string TypeName() => IDataType.GetDataTypeName(this);
}

public class FloatType : IDataType
{
    public string TypeName() => IDataType.GetDataTypeName(this);
}

public class DoubleType : IDataType
{
    public string TypeName() => IDataType.GetDataTypeName(this);
}

public class DecimalType : IDataType
{
    public string TypeName() => IDataType.GetDataTypeName(this);
}

public class StringType : IDataType
{
    public string TypeName() => IDataType.GetDataTypeName(this);
}

public class BinaryType : IDataType
{
    public string TypeName() => IDataType.GetDataTypeName(this);
}

public class TimestampType : IDataType
{
    public string TypeName() => IDataType.GetDataTypeName(this);
}

public class TimestampNtzType : IDataType
{
    public string TypeName() => IDataType.GetDataTypeName(this);
}

public class DateType : IDataType
{
    public string TypeName() => IDataType.GetDataTypeName(this);
}

public class UnsupportedType : IDataType
{
    public string TypeName() => IDataType.GetDataTypeName(this);
}


