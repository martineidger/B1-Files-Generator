using B1_files;
using System;
using System.IO;
using System.Text;
using System.Threading.Tasks;

class Program
{
    static async Task Main(string[] args)
    {
        while (true)
        {
            Console.WriteLine("1 - write, 2 - join, 3 - db, 4 - exit");
            int option;
            option = Convert.ToInt32(Console.ReadLine());

            switch (option)
            {
                case 1:
                    await Generator.GenerateFiles();
                    break;
                case 2:
                    await Generator.JoinFiles("01.01");
                    break;
                case 3:
                    await Generator.ExportFilesToDb();
                    break;

                case 4:
                    return;

                default:
                    break;

            }
        }
    }
}