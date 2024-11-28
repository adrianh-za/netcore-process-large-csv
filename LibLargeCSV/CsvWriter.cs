namespace LibLargeCSV;

/// <summary>
/// Csv Writer for writing records to a file.
/// Uses internal StreamWriter to write to file to keep alive for multiple writes.
/// </summary>
public class CsvWriter: IDisposable
{
    private bool _disposed;
    private readonly string _path;
    private StreamWriter? _writer;

    public CsvWriter(string path)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(path);
        _path = path;
    }

    /// <summary>
    /// Write a record to the file.
    /// </summary>
    public async Task WriteRecord<T>(T record, Func<T, string> serializer)
    {
        //Init stream and output file
        if (_writer is null)
            Initialise();

        //Write the record to the file
        await _writer!.WriteLineAsync(serializer(record));
    }

    /// <summary>
    /// Write a line to the file.
    /// </summary>
    public async Task WriteLine(string line)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(line);

        //Init stream and output file
        if (_writer is null)
            Initialise();

        //Write the record to the file
        await _writer!.WriteLineAsync(line);
    }

    /// <summary>
    /// Create the stream writer and ensure the file is created.
    /// </summary>
    /// <exception cref="Exception"></exception>
    private void Initialise()
    {
        if (_writer is not null)
            throw new Exception("Already initialised");

        //Ensure the directory exists
        var directory = Path.GetDirectoryName(_path);
        if (!string.IsNullOrWhiteSpace(directory) && !Directory.Exists(directory))
            Directory.CreateDirectory(directory);

        //Create the file (will overwrite if it exists with blank file)
        var fileStream = new FileStream(_path, FileMode.Create, FileAccess.Write, FileShare.None, 131072, FileOptions.Asynchronous);
        _writer = new StreamWriter(fileStream);
    }

    /// <summary>
    /// Implementing the Dispose method from IDisposable interface
    /// </summary>
    public void Dispose()
    {
        Dispose(true);
        GC.SuppressFinalize(this);
    }

    /// <summary>
    /// Protected implementation of Dispose pattern
    /// </summary>
    protected virtual void Dispose(bool disposing)
    {
        if (_disposed)
            return;

        if (disposing)
        {
            if (_writer is not null)
            {
                _writer.Dispose();
                _writer = null;
            }
        }

        _disposed = true;
    }

    /// <summary>
    /// Destructor to catch if Dispose wasn't called
    /// </summary>
    ~CsvWriter()
    {
        Dispose(false);
    }
}