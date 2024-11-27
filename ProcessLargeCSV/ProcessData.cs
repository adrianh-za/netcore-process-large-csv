using LibLargeCSV;

namespace ReadLargeCSV;

public static class ProcessData
{
    /// <summary>
    /// Split the specified CSV file into chunks of the specified size.
    /// </summary>
    public static async Task<List<string>> ChunkFile(string inputPath, string outputPathFormat, int chunkSize)
    {
        var rowCounter = 0;
        var chunkCounter = 1;

        //Iterate each row in the large CSV and split into chunked files
        var fileChunks = new List<string>();
        CsvWriter? csvWriter = null;
        try
        {
            await foreach (var person in CsvReader.ReadRecords(inputPath, Person.Parse))
            {
                if (rowCounter == 0)
                {
                    csvWriter?.Dispose();
                    csvWriter = new CsvWriter(string.Format(outputPathFormat, chunkCounter));
                    fileChunks.Add(string.Format(outputPathFormat, chunkCounter));
                }

                await csvWriter!.WriteRecord(person, Person.ToLine);

                if (++rowCounter == chunkSize)
                {
                    chunkCounter++;
                    rowCounter = 0;
                }
            }
        }
        finally
        {
            csvWriter?.Dispose();
        }

        return fileChunks;
    }

    /// <summary>
    /// Takes a list of files and sorts the records in each file by ID.
    /// </summary>
    public static async Task SortFiles(IList<string> files)
    {
        foreach (var fileToSort in files)
        {
            await SortFile(fileToSort);
        }
    }

    /// <summary>
    /// Takes a list of files and sorts the records in each file by ID.
    /// The number of parallel processes can be specified.
    /// </summary>
    public static async Task SortFilesParallel(IList<string> files, int parallelProcesses)
    {
        if (parallelProcesses < 1 || parallelProcesses > 10)
            throw new ArgumentOutOfRangeException(nameof(parallelProcesses), "Parallel processes must be between 1 and 10");

        var parallelOptions = new ParallelOptions
        {
            MaxDegreeOfParallelism = parallelProcesses
        };

        await Parallel.ForEachAsync(files, parallelOptions, async (fileToSort, cancellationToken) =>
        {
            await SortFile(fileToSort);
        });
    }

    //Reads from all the chunk files and merges them into a single sorted file
    public static async Task<long> MergeFiles(IList<string> files, string outputPath)
    {
        long totalRecordsProcessed = 0;

        //The writer to write the merged file
        using var mergedWriter = new CsvWriter(outputPath);

        //Get an Enumerator per chunk file
        var chunkReader = files
            .Select(chunk => CsvReader.ReadRecords(chunk, Person.Parse).GetAsyncEnumerator())
            .ToList();

        //Keep track of whether each Enumerator has more records to process
        var chunkMoreRecords = new bool[chunkReader.Count];

        //Move Enumerator.current to first item respectively, and set flag for each Enumerator if it currently has an item to process
        for (var i = 0; i < chunkReader.Count; i++)
        {
            chunkMoreRecords[i] = await chunkReader[i].MoveNextAsync();
        }

        //Process each file until all files are EOF
        while (chunkMoreRecords.Any(more => more))
        {
            Person? person = null;
            var chunkIndex = -1;

            //Iterate each reader to get the next person of lowest ID
            for (var i = 0; i < chunkReader.Count; i++)
            {
                if (!chunkMoreRecords[i])
                    continue;

                //Process the record
                var currentPerson = chunkReader[i].Current;
                if (person == null || currentPerson.ID < person.ID)
                {
                    person = currentPerson;
                    chunkIndex = i;
                }
            }

            //Store person in merged file, move the chunk reader onto next record (if not EOF).
            if (person is not null)
            {
                await mergedWriter.WriteRecord(person, Person.ToLine);
                totalRecordsProcessed++;
                chunkMoreRecords[chunkIndex] = await chunkReader[chunkIndex].MoveNextAsync();
            }
        }

        //Ensure all enumerators are disposed
        foreach (var reader in chunkReader)
        {
            await reader.DisposeAsync();
        }

        return totalRecordsProcessed;
    }

    /// <summary>
    /// Sorts the records in specified file by ID.
    /// </summary>
    private static async Task SortFile(string path)
    {
        //Load the records and sort
        var personList = (await CsvReader.ReadAllRecords<Person>(path, Person.Parse))
            .OrderBy(p => p.ID)
            .ToList();

        //Write back sorted records to the file
        using var csvWriterChunk = new CsvWriter(path);
        foreach (var person in personList)
        {
            await csvWriterChunk.WriteRecord(person, Person.ToLine);
        }
    }
}