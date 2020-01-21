using System;
using Microsoft.Spark.Sql;
using static Microsoft.Spark.Sql.Functions;
using Column = Microsoft.Spark.Sql.Column;

namespace SparkClient
{
    class Program
    {
        static void Main(string[] args)
        {
            var dataPath = "data/traffic-data.csv";
            var Spark = SparkSession
                           .Builder()
                           .GetOrCreate();

            var dataFrame = Spark
                .Read()
                .Option("header", true)
                .Csv(dataPath);

            GetCountriesSortedByMostSevereWinter(dataFrame, 2017);
            ShowWeatherData(dataFrame);
        }

        static void GetCountriesSortedByMostSevereWinter(DataFrame dataFrame, int year)
        {
            var winterStart = $"{year}-12-22 00:00:00";
            var winterEnd = $"{year + 1}-3-20 00:00:00";

            dataFrame
               .Filter(
                   Col("Source").EqualTo("W")
                       .And(Col("Severity").EqualTo("Severe"))
                       .And(Col("Type").EqualTo("Cold"))
                       .And(Col("StartTime(UTC)").Gt(Lit(winterStart))
                       .And(Col("EndTime(UTC)").Lt(Lit(winterEnd))
               )))
               .GroupBy("State")
               .Agg(Count(dataFrame["State"]))
               .Sort(-Count(dataFrame["State"]))
               .Show(50);
        }

        static void ShowWeatherData(DataFrame dataFrame)
        {
            string city = "San Francisco";
            string startDate = "2010-01-01 00:00:00";
            string endDate = "2020-01-01 00:00:00";
            
            Func<Column, Column> convertSeverity = Udf<string, int>(severityString =>
            {
                switch (severityString)
                {
                    case "Light":
                        return 1;
                    case "Moderate":
                        return 2;
                    case "Heavy":
                        return 3;
                    case "Severe":
                        return 4;
                    default:
                        return 0;
                }
            });

            dataFrame.Filter(
                    Col("Source").EqualTo("W")
                    .And(Col("Severity").IsIn("Light", "Moderate", "Heavy", "Severe"))
                    .And(Col("City") == city)
                    .And(Col("StartTime(UTC)").Between(startDate, endDate))
                    .And(Col("EndTime(UTC)").Between(startDate, endDate))
                )
                .GroupBy("Type")
                .Agg(
                    Min(convertSeverity(dataFrame["Severity"])),
                    Max(convertSeverity(dataFrame["Severity"])),
                    Avg(convertSeverity(dataFrame["Severity"])),
                    Stddev(convertSeverity(dataFrame["Severity"])))
                .Show();
        }
    }
}
