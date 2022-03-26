namespace TwinDataLoader
{
    using System;
    using System.Collections.Generic;
    using System.Diagnostics;
    using System.Linq;
    using System.Net.Http;
    using System.Text.Json;
    using System.Threading.Tasks;

    class Program
    {


        public static async Task Main(string[] args)
        {
            await HexIndexer.IngestDataHexastore();
        }

        public static void TestDataGeneration(string[] args)
        {
            {
                var timer = Stopwatch.StartNew();
                var test = new DataGenerator(4, 4, "contains", 50, 5, false);
                var items = test.Generate();
                Console.WriteLine($"Generated in {timer.ElapsedMilliseconds}");
                Console.WriteLine(JsonSerializer.Serialize(test.GetCount()));
                Console.WriteLine(items.First().Properties["temperature"]);
                Console.WriteLine(items.First().Properties["temperature"]);
                items = test.Update();
                Console.WriteLine($"Updated in {timer.ElapsedMilliseconds}");
                Console.WriteLine(items.First().Properties["temperature"]);
                Console.WriteLine(items.First().Properties["temperature"]);
                Console.WriteLine(JsonSerializer.Serialize(test.GetCount()));
                items = test.Update();
                Console.WriteLine(items.First().Properties["temperature"]);
                Console.WriteLine(items.First().Properties["temperature"]);
                items = test.Update();
                Console.WriteLine(items.First().Properties["temperature"]);
                Console.WriteLine(items.First().Properties["temperature"]);
            }

            {
                var timer = Stopwatch.StartNew();
                var test = new DataGenerator(10, 4, "contains", 50, 5, false);
                test.Generate();
                Console.WriteLine($"Generated in {timer.ElapsedMilliseconds}");
                Console.WriteLine(JsonSerializer.Serialize(test.GetCount()));
                test.Update();
                Console.WriteLine($"Updated in {timer.ElapsedMilliseconds}");
                Console.WriteLine(JsonSerializer.Serialize(test.GetCount()));
            }
        }



        public static async Task QueryHexastore()
        {
            using var httpClient = new HttpClient();
            var url = "http://localhost:5000";
            var graph = $"adt{001}";

            await MakePropertyQuery(url, graph, httpClient);
        }

        public static Task MakePropertyQuery(string url, string graph, HttpClient client)
        {
            return Task.CompletedTask;
        }

        public static async Task IngestDataNeo4J()
        {
            Console.WriteLine("Neo4J Index");
            var data = new DataGenerator(4, 2, "contains", 50, 5, false).Generate();
            var tasks = new List<(Task, Neo4JIndexer)>();
            var reporter = new Neo4JIndexerReporter();
            for (var i = 0; i < 10; i++)
            {
                var neo4JIndexer = new Neo4JIndexer("bolt://localhost:7687", "neo4j", "test", $"adt{i:D3}", reporter);
                var task = neo4JIndexer.IngestAsync(data);
                tasks.Add((task, neo4JIndexer));
            }

            await Task.WhenAll(tasks.Select(x => x.Item1));
            reporter.StopAndReport();
            foreach (var indexer in tasks.Select(x => x.Item2))
            {
                indexer.Dispose();
            }
        }
    }
}
