using Apache.Arrow;
using Apache.Arrow.Ipc;
using Spark.Connect;
using Spark.Connect.Client.Sql;
namespace Spark.Connect.Client.Helpers;

public class ArrowFunctions
{
    public static void ShowArrowBatch(ExecutePlanResponse.Types.ArrowBatch arrowBatch)
    {
        try
        {
            ShowArrowBatchData(arrowBatch.Data.ToByteArray());
        }
        catch (Exception ex)
        {
            // Handle or rethrow exception as needed
            throw new InvalidOperationException("Failed to show arrow batch", ex);
        }
    }

    public static void ShowArrowBatchData(byte[] data)
    {
        var rows = ReadArrowBatchData(data, null);
        foreach (var row in rows)
        {
            var values = row.Values();
            Console.WriteLine(string.Join(", ", values));
        }
    }

    public static List<IRow> ReadArrowBatchData(byte[] data, StructType? schema)
    {
        using var stream = new MemoryStream(data);
        using var reader = new ArrowStreamReader(stream);

        RecordBatch recordBatch;
        var rows = new List<IRow>();
        while ((recordBatch = reader.ReadNextRecordBatch()) != null)
        {
            var values = ReadArrowRecord(recordBatch);
            foreach (var value in values)
            {
                var row = new GenericRowWithSchema(value, schema);
                rows.Add(row);
            }

        }

        return rows;
    }

    private static object?[][] ReadArrowRecord(RecordBatch recordBatch)
    {
        var numRows = recordBatch.Length;
        var numColumns = recordBatch.ColumnCount;
        var values = new object?[numRows][];

        for (int row = 0; row < recordBatch.Length; row++)
            values[row] = new object[numColumns];


        var col = 0;
        foreach (var arrowArray in recordBatch.Arrays)
        {
            var arrow = recordBatch.Column(col);

            void Transfer<T>(Func<int, T> getValue)
            {
                for (int row = 0; row < recordBatch.Length; row++)
                    values[row][col] = getValue(row);
            }


            if (arrow is Int8Array int8Array)
                Transfer(int8Array.GetValue);
            else if (arrow is Int16Array int16array)
                Transfer(int16array.GetValue);
            else if (arrow is Int32Array int32array)
                Transfer(int32array.GetValue);
            else if (arrow is Int64Array int64array)
                Transfer(int64array.GetValue);
            else if (arrow is UInt8Array uint8array)
                Transfer(uint8array.GetValue);
            else if (arrow is UInt16Array uint16array)
                Transfer(uint16array.GetValue);
            else if (arrow is UInt32Array uint32array)
                Transfer(uint32array.GetValue);
            else if (arrow is UInt64Array uint64array)
                Transfer(uint64array.GetValue);
            else if (arrow is FloatArray floatarray)
                Transfer(floatarray.GetValue);
            else if (arrow is DoubleArray doublearray)
                Transfer(doublearray.GetValue);
            else if (arrow is HalfFloatArray halffloatarray)
                Transfer(halffloatarray.GetValue);
            else if (arrow is StringArray stringarray)
                Transfer(i => stringarray.GetString(i, null));
            //else if (arrow is BinaryArray binaryarray)
            //    Transfer(binaryarray.);
            else if (arrow is TimestampArray timestamparray)
                Transfer(timestamparray.GetValue);
            else if (arrow is Date32Array date32array)
                Transfer(date32array.GetValue);
            else if (arrow is Date64Array date64array)
                Transfer(date64array.GetValue);
            //else if (arrow is Decimal128Array decimal128array) 
            //transdecimal128arrayoArray.GetValue());
            //else if (arrow is Decimal256Array decimal256array) 
            //transdecimal256arrayoArray.GetValue());
            else if (arrow is Time32Array time32array)
                Transfer(time32array.GetValue);
            else if (arrow is Time64Array time64array)
                Transfer(time64array.GetValue);
            //else if (arrow is ListArray listarray) 
            //translistarrayoArray.GetValue());
            //else if (arrow is StructArray structarray) 
            //transstructarrayoArray.GetValue());
            //else if (arrow is UnionArray unionarray) 
            //transunionarrayoArray.GetValue());
            //else if (arrow is MapArray maparray) 
            //transmaparrayoArray.GetValue());
            else if (arrow is DurationArray durationarray)
                Transfer(durationarray.GetValue);
            //else if (arrow is IntervalArray intervalarray) 
            //transintervalarrayoArray.GetValue());


            col++;
        }

        return values;
    }
}
