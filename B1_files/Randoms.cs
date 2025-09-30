using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace B1_files
{
    public static class Randoms
    {
        private static readonly string RU = "абвгдеёжзийклмнопрстуфхцчшщъыьэюя" +
                                            "АБВГДЕЁЖЗИЙКЛМНОПРСТУФХЦЧШЩЪЫЬЭЮЯ";
        private static readonly string ENG = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ";
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

        public static int GetIntNumber(int max)
        {
            return Random.Shared.Next(1, max + 1);
        }

        public static double GetDoubleNumber(int intPartMax, int fractPartLength)
        {
            var intPart = Random.Shared.Next(1, intPartMax + 1);

            int scale = (int)Math.Pow(10, fractPartLength);
            int fractPart = Random.Shared.Next(0, scale);

            double result = Math.Round(intPart + (double)fractPart / scale, fractPartLength);
            return result;
        }

        public static DateOnly GetDate(int year)
        {
            var currentDate = DateTime.UtcNow;
            var startDate = new DateTime(currentDate.Year - year, currentDate.Month, currentDate.Day);

            var diff = currentDate - startDate;
            var randDays = Random.Shared.Next(diff.Days);

            var randDate = startDate.AddDays(randDays);

            return DateOnly.FromDateTime(randDate);
        }
    }
}
