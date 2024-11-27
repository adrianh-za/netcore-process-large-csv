using System.Diagnostics;
using ReadLargeCSV;

const string path = @"D:\temp\large-person.csv";
const string chunkPath = @"D:\temp\\chunks\\large-person-{0}.csv";
const string mergePath = @"D:\temp\large-person-merged.csv";
const int chunkSize = 200_000;

//Split file into chunks
var sw = new Stopwatch();
sw.Start();
var chunkedFiles = await ProcessData.ChunkFile(path, chunkPath, chunkSize);
sw.Stop();
Console.WriteLine($"Chunked into {chunkedFiles.Count} files in {sw.Elapsed}");

//Sort the chunked files individually
sw.Restart();
//await ProcessData.SortFiles(chunkedFiles);
await ProcessData.SortFilesParallel(chunkedFiles, 3);
sw.Stop();
Console.WriteLine($"Sorted {chunkedFiles.Count} files in {sw.Elapsed}");

//Merge the sorted files
sw.Restart();
var totalRecords = await ProcessData.MergeFiles(chunkedFiles, mergePath);
sw.Stop();
Console.WriteLine($"Merged {totalRecords} rows to file in {sw.Elapsed}: " + mergePath);