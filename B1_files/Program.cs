using B1_files;
using System;
using System.IO;
using System.Text;
using System.Threading.Tasks;

class Program
{
    //точка входа в программу и главный метод
    static async Task Main(string[] args)
    {
        //блок для отлова исключений
        try
        {
            //цикл для интерактивной работы
            while (true)
            {
                Console.WriteLine("1 - write, 2 - join, 3 - db, 4 - exit");
                int option;
                option = Convert.ToInt32(Console.ReadLine());

                //на основании введненного числа вызываем нужный метод для работы
                switch (option)
                {
                    case 1:
                        await Generator.GenerateFiles();
                        break;
                    case 2:
                        await Generator.JoinFiles("01.01"); //паттерн, который нужно удалить из файлов
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
        catch(System.FormatException ex)
        {
            Console.WriteLine("Please, enter only proposed options (1,2,3 or 4)");
        }
        catch (Exception ex)
        {
            Console.WriteLine("Caught exception: " + ex.ToString());
        }
        
    }
}