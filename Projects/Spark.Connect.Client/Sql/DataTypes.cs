
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

public record BooleanType : IDataType
{
    public string TypeName() => IDataType.GetDataTypeName(this);
}

public record ByteType : IDataType
{
    public string TypeName() => IDataType.GetDataTypeName(this);
}

public record ShortType : IDataType
{
    public string TypeName() => IDataType.GetDataTypeName(this);
}

public record IntegerType : IDataType
{
    public string TypeName() => IDataType.GetDataTypeName(this);
}

public record LongType : IDataType
{
    public string TypeName() => IDataType.GetDataTypeName(this);
}

public record FloatType : IDataType
{
    public string TypeName() => IDataType.GetDataTypeName(this);
}

public record DoubleType : IDataType
{
    public string TypeName() => IDataType.GetDataTypeName(this);
}

public record DecimalType : IDataType
{
    public string TypeName() => IDataType.GetDataTypeName(this);
}

public record StringType : IDataType
{
    public string TypeName() => IDataType.GetDataTypeName(this);
}

public record BinaryType : IDataType
{
    public string TypeName() => IDataType.GetDataTypeName(this);
}

public record TimestampType : IDataType
{
    public string TypeName() => IDataType.GetDataTypeName(this);
}

public record TimestampNtzType : IDataType
{
    public string TypeName() => IDataType.GetDataTypeName(this);
}

public record DateType : IDataType
{
    public string TypeName() => IDataType.GetDataTypeName(this);
}

public record UnsupportedType(object TypeInfo) : IDataType
{
    public string TypeName() => IDataType.GetDataTypeName(this);
}


