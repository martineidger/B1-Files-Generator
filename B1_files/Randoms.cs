using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace B1_files
{
    //класс для генерации случайных строк и чисел
    public static class Randoms
    {
        //символы для генерации случайных строк
        private static readonly string RU = "абвгдеёжзийклмнопрстуфхцчшщъыьэюя" +
                                            "АБВГДЕЁЖЗИЙКЛМНОПРСТУФХЦЧШЩЪЫЬЭЮЯ";
        private static readonly string ENG = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ";

        //получаем необходимое количесвто символов по индексу (случ значение)
        public static string GetRussianSymbols(int length)
        {
            var result = new StringBuilder(length);
            for (int i = 0; i < length; i++)
            {
                var index = Random.Shared.Next(RU.Length);
                result.Append(RU[index]);
            }

            return result.ToString();
        }

        //получаем необходимое количесвто символов по индексу (случ значение)
        public static string GetEnglishSymbols(int length)
        {
            var result = new StringBuilder(length);
            for (int i = 0; i < length; i++)
            {
                var index = Random.Shared.Next(ENG.Length);
                result.Append(ENG[index]);
            }
            return result.ToString();
        }

        //получаем случайное число, используя встроенную библиотеку
        public static int GetIntNumber(int max)
        {
            return Random.Shared.Next(1, max + 1);
        }

        //случайное дробное число
        public static double GetDoubleNumber(int intPartMax, int fractPartLength)
        {
            //генерация целой части числа
            var intPart = Random.Shared.Next(1, intPartMax + 1);

            //генерация дробной части заданной длины
            int scale = (int)Math.Pow(10, fractPartLength);
            int fractPart = Random.Shared.Next(0, scale);

            //вычисление итогового числа
            double result = Math.Round(intPart + (double)fractPart / scale, fractPartLength);
            return result;
        }

        //случайная дата
        public static DateOnly GetDate(int year)
        {
            //вычисление начальной даты (заданное число лет назад)
            var currentDate = DateTime.UtcNow;
            var startDate = new DateTime(currentDate.Year - year, currentDate.Month, currentDate.Day);

            //генерация дней в вычисленном диапазоне
            var diff = currentDate - startDate;
            var randDays = Random.Shared.Next(diff.Days);

            //итоговая дата -> на основе начальной даты и случайного числа дней
            var randDate = startDate.AddDays(randDays);

            return DateOnly.FromDateTime(randDate);
        }
    }
}
