namespace LibLargeCSV;

/// <summary>
/// Represent a single person record.
/// </summary>
public class Person
{
    public required long ID { get; init; }
    public required string Name { get; init; }
    public required DateTime DateOfBirth { get; init; }
    public required string CountryOfBirth { get; init; }

    public Person() { }

    public Person(long id, string name, DateTime dateOfBirth, string countryOfBirth)
    {
        ID = id;
        Name = name;
        DateOfBirth = dateOfBirth;
        CountryOfBirth = countryOfBirth;
    }

    /// <summary>
    /// Convert comma-separated line to a Person object.
    /// </summary>
    public static Person Parse(string line)
    {
        var parts = line.Split(',');
        return new Person()
        {
            ID = long.Parse(parts[0]),
            Name = parts[1],
            DateOfBirth = DateTime.Parse(parts[2]),
            CountryOfBirth = parts[3]
        };
    }

    /// <summary>
    /// Converts a Person object to a comma-separated line.
    /// </summary>
    public static string ToLine(Person person)
    {
        return $"{person.ID},{person.Name},{person.DateOfBirth:yyyy-MM-dd},{person.CountryOfBirth}";
    }
}