namespace TwinDataLoader
{
    using System;
    using System.Collections.Concurrent;
    using System.Collections.Generic;
    using System.Diagnostics;
    using System.Linq;
    using System.Net.Http;
    using System.Net.Http.Json;
    using System.Text.Json.Serialization;
    using System.Threading.Tasks;

    public class HexIndexer : IDisposable
    {
        private readonly HttpClient httpClient;

        private readonly string url;
        private readonly string instance;
        private readonly HexIndexerReporter reporter;
        private readonly int batchSize = 1_000;

        public static async Task IngestDataHexastore()
        {
            Console.WriteLine("Hexastore Index");
            var numOfGraphs = 20;
            var dataGenerator = new DataGenerator(level: 12, factor: 2, relationshipName: "has", numTwinProperties: 100, numEdgeProperties: 100, false);
            var data = dataGenerator.Generate().ToList();

            var reporter = new HexIndexerReporter();

            var indexers = new List<HexIndexer>();
            for (var i = 0; i < numOfGraphs; i++)
            {
                var hexIndexer = new HexIndexer("http://localhost:5000", $"adt{i:D3}", reporter);
                indexers.Add(hexIndexer);
                Console.WriteLine($"Indexer {i}");
            }

            var tasks = indexers.Select(x => (x.IngestAsync(data), x)).ToList();
            await Task.WhenAll(tasks.Select(x => x.Item1));
            reporter.StopAndReport();
            foreach (var indexer in tasks.Select(x => x.Item2))
            {
                indexer.Dispose();
            }
        }

        public HexIndexer(string url, string instance, HexIndexerReporter reporter)
        {
            httpClient = new HttpClient();
            this.url = url;
            this.instance = instance;
            this.reporter = reporter;
            var rsp = httpClient.GetAsync($"{url}/api/store/{instance}/0").Result;
            rsp.EnsureSuccessStatusCode();
        }

        public async Task IngestAsync(List<GraphEntity> data)
        {
            Console.WriteLine($"Starting for instance {instance}");
            var list = data;
            var marker = 0;

            var timer = Stopwatch.StartNew();
            while (marker < list.Count)
            {
                var take = list.Skip(marker).Take(batchSize).ToList();
                if (!take.Any())
                {
                    break;
                }

                reporter.Start();

                var rsp = await httpClient.PostAsync($"{url}/api/store/{instance}/twin", JsonContent.Create(take));
                ProcessResponse(rsp);
                Console.WriteLine($"{instance} Created {take.Count()} nodes");
                reporter.AddNodes(instance, take.Count());

                marker += batchSize;
                Console.WriteLine($"Completed ingestion of {marker} entities in {timer.Elapsed.TotalSeconds} seconds.");
            }
        }

        private void ProcessResponse(HttpResponseMessage rsp)
        {
            rsp.EnsureSuccessStatusCode();
            Console.WriteLine(rsp.StatusCode);
        }

        public void Dispose()
        {
            if (this.httpClient != null)
            {
                this.httpClient.Dispose();
            }
        }
    }

    public class UpdateRquest
    {
        [JsonPropertyName("partitionKey")]
        public string PartitionKey { get; set; }

        [JsonPropertyName("data")]
        public object Data { get; set; }
    }

    public class HexIndexerReporter
    {
        Stopwatch watch;
        readonly ConcurrentDictionary<string, Int32> counts = new ConcurrentDictionary<string, Int32>();
        public void Start()
        {
            if (watch == null)
            {
                watch = Stopwatch.StartNew();
            }
        }

        public void AddNodes(string instance, int v)
        {
            var rsp = counts.AddOrUpdate(instance, v, (instance, x) => { return x + v; });
        }

        public void AddEdges(string instance, int v)
        {
            AddNodes(instance, v);
        }

        public void StopAndReport()
        {
            watch.Stop();
            var total = 0;
            foreach (var item in counts)
            {
                Console.WriteLine($"{item.Key} {item.Value}");
                total += item.Value;
            }

            Console.WriteLine($"{nameof(HexIndexerReporter)}: Entities added {total} in {watch.Elapsed.TotalSeconds} seconds");
            Console.WriteLine($"{nameof(HexIndexerReporter)}: Rate added {total / watch.Elapsed.TotalSeconds} items/second");
        }
    }
}
