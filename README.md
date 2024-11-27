This was done for fun!

The purpose of the app is to sort a large unsorted (ten million records - 500MB) csv file without loading the entire file into memory and sorting,

The app will chunk the large CSV file into smaller files.  Each file is then sorted in parallel. Once all the chunked files are sorted, they are then merged into a single sorted CSV file.

The solution consists of two consoles apps, and a single class library

 - GenerateLargeCSV
	 - This is for generating the large, unsorted CSV.

 - ProcessLargeCSV
	 - This is for sorting the large, unsorted CSV.

 - LibLargeCSV
	 - The lib containing the CSV utils and data type.
