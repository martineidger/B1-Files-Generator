using Npgsql;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace B1_files
{
    public static class Generator
    {
        const string DIR_PATH = "files";
        const string FILE_NAME = "file_";
        const string FILE_JOIN_NAME = "report.txt";
        const int FILES_COUNT = 20;
        const int ROWS_COUNT = 100000;
        const string DIVIDER = "||";

        const int YEARS_AGO = 5;
        const int RU_COUNT = 10;
        const int ENG_COUNT = 10;
        const int INT_RANDOM_MAX = 100000000;
        const int DOUBLE_RANDOM_MAX = 20;
        const int DOUBLE_RANDOM_FRACT = 8;

        const int MAX_SYMBOLS_IN_ROW = 57;
        const int BATCH_SIZE = 1000;
        public static async Task GenerateFiles()
        {
            if (!Directory.Exists(DIR_PATH))
                Directory.CreateDirectory(DIR_PATH);

            for (int i = 1; i <= FILES_COUNT; i++)
            {
                string fileName = $"{FILE_NAME}{i}.txt";
                string filePath = Path.Combine(DIR_PATH, fileName);

                using var fs = new FileStream(filePath, FileMode.Create, FileAccess.Write, FileShare.None, bufferSize: 1 << 20, useAsync: true);
                using var sw = new StreamWriter(fs, Encoding.UTF8, bufferSize: 1 << 16);

                var stringRow = new StringBuilder(MAX_SYMBOLS_IN_ROW * BATCH_SIZE);

                for (int j = 0; j < ROWS_COUNT; j++)
                {
                    var date = Randoms.GetDate(YEARS_AGO);
                    var engString = Randoms.GetEnglishSymbols(ENG_COUNT);
                    var ruString = Randoms.GetRussianSymbols(RU_COUNT);
                    var intRandom = Randoms.GetIntNumber(INT_RANDOM_MAX);
                    var doubleRandom = Randoms.GetDoubleNumber(DOUBLE_RANDOM_MAX, DOUBLE_RANDOM_FRACT);

                    stringRow.Append(date)
                        .Append(DIVIDER)
                        .Append(engString)
                        .Append(DIVIDER)
                        .Append(ruString)
                        .Append(DIVIDER)
                        .Append(intRandom)
                        .Append(DIVIDER)
                        .Append(doubleRandom)
                        .Append(DIVIDER)
                        .AppendLine();

                    if ((j + 1) % BATCH_SIZE == 0)
                    {
                        await sw.WriteAsync(stringRow.ToString());
                        stringRow.Clear();
                    }
                }


                if (stringRow.Length > 0)
                {
                    await sw.WriteAsync(stringRow.ToString());
                    stringRow.Clear();
                }

                await sw.FlushAsync();
                Console.WriteLine($"Written {fileName}");
            }

        }

        public static async Task JoinFiles(string pattern)
        {
            if (!Directory.Exists(DIR_PATH))
                throw new DirectoryNotFoundException(DIR_PATH);

            string filePath = Path.Combine(DIR_PATH, FILE_JOIN_NAME);

            using var fs = new FileStream(filePath, FileMode.Create, FileAccess.Write, FileShare.None, bufferSize: 1 << 20, useAsync: true);
            using var sw = new StreamWriter(fs, Encoding.UTF8, bufferSize: 1 << 16);


            string[] files = Directory.GetFiles(DIR_PATH, "file_*.txt")
                          .OrderBy(f => f)
                          .ToArray();

            var stringRows = new StringBuilder(BATCH_SIZE);
            int rowsCount = 0;
            int skipCount = 0;

            foreach (string file in files)
            {
                string tempFile = Path.GetTempFileName();
                using (var sr = new StreamReader(file))
                {
                    using (var tsw = new StreamWriter(tempFile))
                    {

                        string line;
                        while ((line = await sr.ReadLineAsync()) != null)
                        {
                            if (!line.Contains(pattern))
                            {
                                stringRows.AppendLine(line);
                                rowsCount++;
                            }
                            else
                                skipCount++;

                            if (rowsCount % BATCH_SIZE == 0)
                            {
                                string chunk = stringRows.ToString();
                                await sw.WriteAsync(chunk);
                                await tsw.WriteAsync(chunk);
                                stringRows.Clear();

                            }
                        }

                        if (stringRows.Length > 0)
                        {
                            string chunk = stringRows.ToString();
                            await sw.WriteAsync(chunk);
                            await tsw.WriteAsync(chunk);
                            stringRows.Clear();
                        }

                        Console.WriteLine($"File {file} joined");
                    }
                }

                File.Delete(file);
                File.Move(tempFile, file);
            }

            await sw.FlushAsync();
            Console.WriteLine($"Written {rowsCount} lines in '{FILE_JOIN_NAME}' file. Skipped {skipCount} lines");
        }

        public static async Task ExportFilesToDb()
        {
            if (!Directory.Exists(DIR_PATH))
                throw new DirectoryNotFoundException(DIR_PATH);

            string filePath = Path.Combine(DIR_PATH, FILE_JOIN_NAME);

            string[] files = Directory.GetFiles(DIR_PATH, "file_*.txt")
                          .OrderBy(f => f)
                          .ToArray();

            var stringRows = new StringBuilder(BATCH_SIZE);
            int rowsCount = 0;

            var connString = "Host=localhost;Port=5432;Username=postgres;Password=Qwerty1234;Database=b1_db";

            await using var conn = new NpgsqlConnection(connString);
            await conn.OpenAsync();
            var totalWrittenCount = 0;
            var writtenCount = 0;

            foreach (string file in files)
            {
                using var sr = new StreamReader(file);
                using var writer = conn.BeginTextImport(
                    "COPY Reports (date_field, eng_field, ru_field, number_field, decimal_field) " +
                    "FROM STDIN (FORMAT CSV, DELIMITER E'|', NULL '', HEADER FALSE)"
                    );

                string? line;

                //int fileTotalCount = File.ReadLines(file).Count();

                var totalCount = files.Sum(f => File.ReadLines(f).Count()); ;
                while ((line = await sr.ReadLineAsync()) != null)
                {
                    if (string.IsNullOrWhiteSpace(line)) continue;

                    line = line.Replace("||", "|");
                    line = line.Replace(',', '.');

                    if (line.EndsWith("|"))
                        line = line.Substring(0, line.Length - 1);

                    await writer.WriteLineAsync(line);
                    totalWrittenCount++;
                    writtenCount++;
                    //Console.Clear();
                    //Console.WriteLine($"Written: {totalWrittenCount}/{totalCount} lines");

                    //writtenCount++;
                    //if (writtenCount % 100 == 0)
                    //{
                    //    Console.WriteLine($"Файл {Path.GetFileName(file)}: загружено {writtenCount}/{fileTotalCount} строк");
                    //}

                    if (totalWrittenCount % 1000 == 0 || totalWrittenCount == totalCount)
                    {
                        Console.SetCursorPosition(0, Console.CursorTop);
                        Console.Write($"Written: {totalWrittenCount}/{totalCount} lines");
                    }
                }

                await writer.DisposeAsync();
                Console.WriteLine($" / Файл {Path.GetFileName(file)} загружен полностью: {writtenCount} строк");
                writtenCount = 0;

            }


        }
    }
}
