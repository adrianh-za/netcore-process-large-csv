using System.Diagnostics;
using ReadLargeCSV;

const string path = @"D:\temp\large-person.csv";
const string chunkPath = @"D:\temp\\chunks\\large-person-{0}.csv";
const string mergePath = @"D:\temp\large-person-merged.csv";
const int chunkSize = 1_000_000;

//Split file into chunks
var sw = new Stopwatch();
sw.Start();

// ** Does convert string <> Person, slower but more type safe (1m 49s)
//var chunkedFiles = await ProcessData.ChunkFile(path, chunkPath, chunkSize);

// ** Does not convert string <> Person, merely reads and writes the lines (0m 40s)
var chunkedFiles = await ProcessData.ChunkFileRaw(path, chunkPath, chunkSize);

sw.Stop();
Console.WriteLine($"Chunked into {chunkedFiles.Count} files in {sw.Elapsed}");


//Sort the chunked files individually
sw.Restart();

// ** Single threaded sort is slower (3m 30s)
//await ProcessData.SortFiles(chunkedFiles);
//await ProcessData.SortFilesParallel(chunkedFiles, 1);

// ** Multi-threaded is fastest with 2 threads (2m 19s)
await ProcessData.SortFilesParallel(chunkedFiles, 2);   //Most performant

sw.Stop();
Console.WriteLine($"Sorted {chunkedFiles.Count} files in {sw.Elapsed}");

//Merge the sorted files
sw.Restart();
var totalRecords = await ProcessData.MergeFiles(chunkedFiles, mergePath);
sw.Stop();
Console.WriteLine($"Merged {totalRecords} rows to file in {sw.Elapsed}: " + mergePath);