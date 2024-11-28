using System.Diagnostics;
using Bogus;
using LibLargeCSV;
using Person = LibLargeCSV.Person;

namespace GenerateLargeCSV;

/// <summary>
/// For generating a large CSV file with random data
/// </summary>
public class GenerateData(string outputFile, int rowCount)
{
    /// <summary>
    /// Generates a CSV file with random data of type <see cref="Person"/>
    /// </summary>
    public async Task<string> Execute(int fakeBatchSize = 10000)
    {
        var sw = new Stopwatch();

        //Generate the list of unique IDs upfront for the number of records specified and put into a queue for easy popping
        sw.Start();
        var uniqueIdsQueue = new Queue<long>(GetUniqueIds(rowCount));
        sw.Stop();
        Console.WriteLine($"Generated {rowCount} unique IDs in {sw.Elapsed}");

        //Creates a new file for the data (will overwrite if already exists)
        using var csvWriter = new CsvWriter(outputFile);

        //Create a data row and append into file created
        sw.Restart();
        var batchRowCounter = 0;
        var rowCounter = 0;
        while (rowCounter < rowCount)
        {
            //Generate records in batches as specified (or less if remaining records are less than batch size) for performance gains
            var recordsToGenerate = Math.Min(fakeBatchSize, rowCount - rowCounter);

            //Generate fake person
            var fakePerson = new Faker<Person>("en_GB")
                .RuleFor(p => p.ID, f => uniqueIdsQueue.Dequeue())
                .RuleFor(p => p.Name, f => f.Person.FullName)
                .RuleFor(p => p.CountryOfBirth, f => f.Address.Country())
                .RuleFor(p => p.DateOfBirth, f => f.Date.Past(100));

            // Generate a fake person and write to file
            var persons = fakePerson.Generate(recordsToGenerate);

            //Write the records to the file
            foreach (var person in persons)
            {
                await csvWriter.WriteRecord(person, Person.ToLine);
            }

            rowCounter += recordsToGenerate;
            batchRowCounter += recordsToGenerate;

            //Output progress every 100,000 rows
            if (batchRowCounter >= 100_000)
            {
                Console.WriteLine($"Added {batchRowCounter} rows to file in {sw.Elapsed}.  Total rows: {rowCounter} of {rowCount}");
                sw.Restart();
                batchRowCounter = 0;
            }
        }

        Console.WriteLine($"All {rowCount} added to file.");
        return outputFile;
    }

    /// <summary>
    /// Get a unique ID excluding previously generated IDs in the specified hashset.
    /// The new ID will be added to the hashset.
    /// </summary>
    private static long GetUniqueId(HashSet<long> idHash, Random random)
    {
        long id;
        do
        {
            id = random.NextInt64(long.MaxValue);
        } while (!idHash.Add(id));
        return id;
    }

    /// <summary>
    /// Get a list of unique IDs.
    /// </summary>
    private static List<long> GetUniqueIds(int count)
    {
        var ids = new List<long>(count);
        var idHash = new HashSet<long>();
        var random = new Random();

        while (idHash.Count < count)
        {
            var id = GetUniqueId(idHash, random);
            ids.Add(id);
        }

        return ids;
    }
}