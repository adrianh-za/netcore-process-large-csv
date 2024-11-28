namespace LibLargeCSV;

/// <summary>
/// Csv Reader to read records from a file.
/// </summary>
public static class CsvReader
{
    /// <summary>
    /// Reads records from a file, converts to specified type, and returns an enumerator.
    /// </summary>
    public static async IAsyncEnumerable<T> ReadRecords<T>(string path, Func<string, T> deserializer)
    {
        await foreach (var line in ReadLines(path))
        {
            yield return deserializer(line);
        }
    }

    /// <summary>
    /// Reads records from a file, converts to specified type, and returns as a list.
    /// </summary>
    public static async Task<IList<T>> ReadAllRecords<T>(string path, Func<string, T> deserializer)
    {
        var records = new List<T>();
        await foreach(var person in ReadRecords(path, deserializer))
        {
            records.Add(person);
        }
        return records;
    }

    /// <summary>
    /// Read lines from a file as a string and returns an enumerator.
    /// </summary>
    public static async IAsyncEnumerable<string> ReadLines(string path)
    {
        using var reader = new StreamReader(path);
        while (!reader.EndOfStream)
        {
            var row = await reader.ReadLineAsync();
            if (string.IsNullOrWhiteSpace(row))
                continue;

            yield return row;
        }
    }
}