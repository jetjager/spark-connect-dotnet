using Spark.Connect.Client.Sql;
using Spark.Connect;
namespace Spark.Connect.Client.Helpers;

public class Conversions
{

    public static List<StructField> ConvertProtoStructFields(List<DataType.Types.StructField> input)
    {
        List<StructField> result = new List<StructField>(input.Count);
        foreach (var f in input)
        {
            result.Add(ConvertProtoStructField(f));
        }
        return result;
    }

    public static StructField ConvertProtoStructField(DataType.Types.StructField field)
    {
        return new StructField(field.Name, ConvertProtoDataTypeToDataType(field.DataType), field.Nullable);
    }

    // ConvertProtoDataTypeToDataType converts protobuf data type to Spark connect sql data type
    public static IDataType ConvertProtoDataTypeToDataType(DataType input)
    {
        switch (input.KindCase)
        {
            case DataType.KindOneofCase.Boolean:
                return new BooleanType();
            case DataType.KindOneofCase.Byte:
                return new ByteType();
            case DataType.KindOneofCase.Short:
                return new ShortType();
            case DataType.KindOneofCase.Integer:
                return new IntegerType();
            case DataType.KindOneofCase.Long:
                return new LongType();
            case DataType.KindOneofCase.Float:
                return new FloatType();
            case DataType.KindOneofCase.Double:
                return new DoubleType();
            case DataType.KindOneofCase.Decimal:
                return new DecimalType();
            case DataType.KindOneofCase.String:
                return new StringType();
            case DataType.KindOneofCase.Binary:
                return new BinaryType();
            case DataType.KindOneofCase.Timestamp:
                return new TimestampType();
            case DataType.KindOneofCase.TimestampNtz:
                return new TimestampNtzType();
            case DataType.KindOneofCase.Date:
                return new DateType();
            default:
                return new UnsupportedType(input.KindCase);
        }
    }


}