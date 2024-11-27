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
    public async Task<string> Execute(int fakeBatchSize = 5000)
    {
        var idHash = new HashSet<long>();
        var random = new Random();

        //Creates a new file for the data
        using var csvWriter = new CsvWriter(outputFile);

        //Create a data row and append into file created
        var rowCounter = 0;
        while (rowCounter < rowCount)
        {
            //Generate records in batches as specified (or less if remaining records are less than batch size)
            var recordsToGenerate = Math.Min(fakeBatchSize, rowCount - rowCounter);

            //Generate fake person
            var fakePerson = new Faker<Person>("en_GB")
                .RuleFor(p => p.ID, f => GetUniqueId(idHash, random))
                .RuleFor(p => p.Name, f => f.Person.FullName)
                .RuleFor(p => p.CountryOfBirth, f => f.Address.Country())
                .RuleFor(p => p.DateOfBirth, f => f.Date.Past(100));

            // Generate a fake person and write to file
            var persons = fakePerson.Generate(recordsToGenerate);
            foreach(var person in persons)
                await csvWriter.WriteRecord(person, Person.ToLine);
            rowCounter += recordsToGenerate;
        }

        return outputFile;
    }


    /// <summary>
    /// Get a unique ID excluding previously generated IDs
    /// </summary>
    private static long GetUniqueId(HashSet<long> idHash, Random random)
    {
        long id;
        do
        {
            id = random.NextInt64(9999999999999999);
        } while (!idHash.Add(id));
        return id;
    }
}