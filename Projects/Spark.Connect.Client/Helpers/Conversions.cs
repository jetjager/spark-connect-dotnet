using Spark.Connect.Client.Sql;
using Spark.Connect;
using System.Globalization;
using Google.Protobuf.WellKnownTypes;
using System.Collections;
using System.Xml;
using System.Security.Cryptography;
namespace Spark.Connect.Client.Helpers;
using static Spark.Connect.Expression;
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

    private static DateTime unixEpochStart = new DateTime(1970, 1, 1, 0, 0, 0, DateTimeKind.Utc);
    private static DateTime unixEpochStartUnspecified = new DateTime(1970, 1, 1, 0, 0, 0, DateTimeKind.Unspecified);

    public static Types.Literal ConvertObjectToLiteral(object input)
    {
        var lit = new Types.Literal();

        switch (input)
        {
            case null:
                lit.Null = null;
                break;
            case byte[] bytes:
                lit.Binary = Google.Protobuf.ByteString.CopyFrom(bytes);
                break;
            case bool b:
                lit.Boolean = b;
                break;
            case short s:
                lit.Short = s;
                break;
            case byte b:
                lit.Byte = b;
                break;
            case int i:
                lit.Integer = i;
                break;
            case long l:
                lit.Long = l;
                break;
            case float f:
                lit.Float = f;
                break;
            case double d:
                lit.Double = d;
                break;
            case decimal d:
                lit.Decimal = new Types.Literal.Types.Decimal
                {
                    Scale = d.Scale,
                    Value = d.ToString(CultureInfo.InvariantCulture)
                };
                break;
            case string str:
                lit.String = str;
                break;
            case DateTime dt when dt.TimeOfDay.Seconds == 0:
                lit.Date = (int)(dt.ToUniversalTime() - unixEpochStart).TotalDays;
                break;
            case DateTime dt when dt.Kind == DateTimeKind.Unspecified:
                lit.TimestampNtz = (long)(dt - unixEpochStartUnspecified).TotalMicroseconds;
                break;
            case DateTime dt when dt.Kind == DateTimeKind.Utc:
                lit.TimestampNtz = (long)(dt - unixEpochStart).TotalMicroseconds;
                break;
            case DateTime dt when dt.Kind == DateTimeKind.Local:
                lit.Timestamp = (long)(dt.ToUniversalTime() - unixEpochStart).TotalSeconds;
                break;
            case TimeSpan ts: // This also handles YearMonthInterval and DayTimeInterval I guess
                lit.CalendarInterval = new Types.Literal.Types.CalendarInterval
                {
                    Days = ts.Days,
                    //Months = , //TODO should we set Months?
                    Microseconds = ts.Microseconds
                };
                break;
            case object[] arr:
                // TODO empty array
                lit.Array = new Types.Literal.Types.Array
                {
#pragma warning disable CS8604 // Possible null reference argument.
                    ElementType = GetDataTypeOf(arr.FirstOrDefault()),
#pragma warning restore CS8604 // Possible null reference argument.
                    Elements = { arr.Select(ConvertObjectToLiteral) }
                };
                break;
            // TODO add dictionary type
            default:
                throw new NotImplementedException($"Cannot convert input of {input} to a literal.");
        }

        return lit;
    }

    public static DataType GetDataTypeOf(object obj)
    {
        var dt = new DataType();

        switch (obj)
        {
            case null:
                dt.Null = null;
                break;
            case byte[]:
                dt.Binary = new DataType.Types.Binary();
                break;
            case bool:
                dt.Boolean = new DataType.Types.Boolean();
                break;
            case byte:
                dt.Byte = new DataType.Types.Byte();
                break;
            case short:
                dt.Short = new DataType.Types.Short();
                break;
            case int:
                dt.Integer = new DataType.Types.Integer();
                break;
            case long:
                dt.Long = new DataType.Types.Long();
                break;
            case float:
                dt.Float = new DataType.Types.Float();
                break;
            case double:
                dt.Double = new DataType.Types.Double();
                break;
            case decimal:
                dt.Decimal = new DataType.Types.Decimal();
                break;
            case string:
                dt.String = new DataType.Types.String();
                break;
            case char:
                dt.Char = new DataType.Types.Char();
                break;
            // TODO is varchar a type in C#?

            case DateTime dateTime when dateTime.Second == 0:
                dt.Date = new DataType.Types.Date();
                break;

            case DateTime dateTime when dateTime.Kind == DateTimeKind.Unspecified:
                dt.TimestampNtz = new DataType.Types.TimestampNTZ();
                break;
            case DateTime:
                dt.Timestamp = new DataType.Types.Timestamp();
                break;
            case TimeSpan:
                dt.CalendarInterval = new DataType.Types.CalendarInterval();
                break;
            // TODO YearMonthInterval and DayTimeInterval
            case object[]:
                dt.Array = new DataType.Types.Array();
                break;
            // TODO Map, Struct, and Udt
            default:
                throw new NotImplementedException($"Cannot get datatype for object of type {obj.GetType()}");
        }

        return dt;
    }


}