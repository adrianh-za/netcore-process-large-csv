using System.Diagnostics;
using GenerateLargeCSV;

const string path = @"D:\temp\large-person.csv";
//const int rowCount = 10_000_000;    //About 530MB file - 5 mins
const int rowCount = 100_000_000;    //About 5.3GB file 50 mins

//This will run for a while, so just let it finish
var sw = new Stopwatch();
sw.Start();
var generator = new GenerateData(path, rowCount);
var outputFile = await generator.Execute();
sw.Stop();

Console.WriteLine($"Added {rowCount} rows to file in {sw.Elapsed}: " + outputFile);