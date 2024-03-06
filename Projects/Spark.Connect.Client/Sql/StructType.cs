
namespace Spark.Connect.Client.Sql;

public class StructField
{
    public string Name { get; set; }
    public IDataType DataType { get; set; }
    public bool Nullable { get; set; } = true; // Default value set to true

    public StructField(string name, IDataType dataType, bool nullable = true)
    {
        Name = name;
        DataType = dataType;
        Nullable = nullable;
    }
}

public class StructType
{
    public string TypeName { get; set; }
    public List<StructField> Fields { get; set; } = new List<StructField>();

    public StructType(string typeName, List<StructField> fields = null)
    {
        TypeName = typeName;
        Fields = fields ?? new List<StructField>(); // Initialize with an empty list if null is passed
    }
}
