using Npgsql;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace B1_files
{
    //класс для генерации файлов и работы с ними
    public static class Generator
    {
        //константы 
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

        const string CONN_STRING = "Host=localhost;Port=5432;Username=postgres;Password=Qwerty1234;Database=b1_db";

        //метод, генерирующий файлы
        public static async Task GenerateFiles()
        {
            //проверка существования папки
            if (!Directory.Exists(DIR_PATH))
                Directory.CreateDirectory(DIR_PATH);

            for (int i = 1; i <= FILES_COUNT; i++)
            {
                string fileName = $"{FILE_NAME}{i}.txt";
                string filePath = Path.Combine(DIR_PATH, fileName);

                //открываем потоки для записи и чтения

                using var fs = new FileStream(filePath,   
                    FileMode.Create, 
                    FileAccess.Write, //режим создания или записи,
                    FileShare.None, //запрет общего доступа для работы с файлами
                    bufferSize: 1 << 20, //размер буфера для вставки большого числа строк, 
                    useAsync: true); //поддержка асинхронности

                using var sw = new StreamWriter(fs, 
                    Encoding.UTF8, //кодировка
                    bufferSize: 1 << 16); //размер буфера

                //stringbuilder размером с пачку -> эффективная вставка и генерация
                var stringRow = new StringBuilder(MAX_SYMBOLS_IN_ROW * BATCH_SIZE);

                for (int j = 0; j < ROWS_COUNT; j++)
                {
                    //получение сгенерированных случайных последовательностей
                    var date = Randoms.GetDate(YEARS_AGO);
                    var engString = Randoms.GetEnglishSymbols(ENG_COUNT);
                    var ruString = Randoms.GetRussianSymbols(RU_COUNT);
                    var intRandom = Randoms.GetIntNumber(INT_RANDOM_MAX);
                    var doubleRandom = Randoms.GetDoubleNumber(DOUBLE_RANDOM_MAX, DOUBLE_RANDOM_FRACT);

                    //генерация итоговой строки
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

                    //если размер = размер пачки  -> отправляем
                    if ((j + 1) % BATCH_SIZE == 0)
                    {
                        await sw.WriteAsync(stringRow.ToString());
                        stringRow.Clear();
                    }
                }

                //если остались данные -> дописываем
                if (stringRow.Length > 0)
                {
                    await sw.WriteAsync(stringRow.ToString());
                    stringRow.Clear();
                }

                //выталкивает данные и очищает буферы
                await sw.FlushAsync();
                Console.WriteLine($"Written {fileName}");
            }

        }

        //метод для обьединения файлов
        public static async Task JoinFiles(string pattern)
        {
            if (!Directory.Exists(DIR_PATH))
                throw new DirectoryNotFoundException(DIR_PATH);

            //полный путь к файлам (по шаблону)
            string filePath = Path.Combine(DIR_PATH, FILE_JOIN_NAME);

            //потоки
            using var fs = new FileStream(filePath, FileMode.Create, FileAccess.Write, FileShare.None, bufferSize: 1 << 20, useAsync: true);
            using var sw = new StreamWriter(fs, Encoding.UTF8, bufferSize: 1 << 16);

            //будем работать с файлами заданного шаблона
            string[] files = Directory.GetFiles(DIR_PATH, "file_*.txt")
                          .OrderBy(f => f)
                          .ToArray();

            var stringRows = new StringBuilder(MAX_SYMBOLS_IN_ROW * BATCH_SIZE);
            int rowsCount = 0;
            int skipCount = 0;

            foreach (string file in files)
            {
                //временный файл для очистки файла от строк с заданным шаблоном (параметр в функции)
                string tempFile = Path.GetTempFileName();
                using (var sr = new StreamReader(file))
                {
                    using (var tsw = new StreamWriter(tempFile))
                    {

                        string line;
                        //считываем все строки файла
                        while ((line = await sr.ReadLineAsync()) != null)
                        {
                            //содержит ли заданный шаблон
                            if (!line.Contains(pattern))
                            {
                                stringRows.AppendLine(line);
                                rowsCount++;
                            }
                            else
                                skipCount++;

                            //записываем если достигли размера пачки (запись в общий файл)
                            if (rowsCount % BATCH_SIZE == 0)
                            {
                                string chunk = stringRows.ToString();
                                await sw.WriteAsync(chunk);
                                await tsw.WriteAsync(chunk);
                                stringRows.Clear();

                            }
                        }

                        //дописываем оставшиеся данные (в общий файл)
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

                //записываем почищенные данные обратно в исходный файл
                File.Delete(file);
                File.Move(tempFile, file);
            }

            await sw.FlushAsync();
            Console.WriteLine($"Written {rowsCount} lines in '{FILE_JOIN_NAME}' file. Skipped {skipCount} lines");
        }

        //метод загрузки данных из файлов в бд
        public static async Task ExportFilesToDb()
        {
            if (!Directory.Exists(DIR_PATH))
                throw new DirectoryNotFoundException(DIR_PATH);

            string filePath = Path.Combine(DIR_PATH, FILE_JOIN_NAME);

            string[] files = Directory.GetFiles(DIR_PATH, "file_*.txt")
                          .OrderBy(f => f)
                          .ToArray();

            //создаем и открываем подключение к бд
            await using var conn = new NpgsqlConnection(CONN_STRING);
            await conn.OpenAsync();

            var totalWrittenCount = 0;
            var writtenCount = 0;

            //общее количество строк для статистики загрузки
            var totalCount = files.Sum(f => File.ReadLines(f).Count());

            foreach (string file in files)
            {
                //поток для чтения записей из файла
                using var sr = new StreamReader(file);

                //создаем обьект для массовой загрузки данных в бд
                using var writer = conn.BeginTextImport( //using сам закрывает и подчищает подключение после завершения операции
                    "COPY Reports (date_field, eng_field, ru_field, number_field, decimal_field) " + //название столбцов в бд
                    "FROM STDIN (FORMAT CSV, DELIMITER E'|', NULL '', HEADER FALSE)" //указываем формат, символ разделитель
                    );

                string? line;
                
                //читаем все строки
                while ((line = await sr.ReadLineAsync()) != null)
                {
                    if (string.IsNullOrWhiteSpace(line)) continue;

                    //подготовка строки для загрузки в бд
                    // || -> | т.к. разделитетелем данных может быть один символ
                    // , -> . т.к. в постгрес вещественные числа хранятся с точкой 
                    line = line.Replace("||", "|");
                    line = line.Replace(',', '.');

                    //лишний символ-разделитель в конце строки
                    if (line.EndsWith("|"))
                        line = line.Substring(0, line.Length - 1);

                    //записываем обработанную строку в бд
                    await writer.WriteLineAsync(line);
                    totalWrittenCount++;
                    writtenCount++;
                   
                    //вывод статистики загрузки
                    if (totalWrittenCount % 1000 == 0 || totalWrittenCount == totalCount)
                    {
                        Console.SetCursorPosition(0, Console.CursorTop);
                        Console.Write($"Written: {totalWrittenCount}/{totalCount} lines");
                    }
                }

               
                Console.WriteLine($" / Файл {Path.GetFileName(file)} загружен полностью: {writtenCount} строк");
                writtenCount = 0;

            }


        }
    }
}
