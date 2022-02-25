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

        public HexIndexer(string url, string instance, HexIndexerReporter reporter)
        {
            httpClient = new HttpClient();
            this.url = url;
            this.instance = instance;
            this.reporter = reporter;
        }

        public async Task IngestAsync(IEnumerable<object> data)
        {
            Console.WriteLine($"Starting for instance {instance}");
            var list = data.ToList();
            var marker = 0;

            var batchSize = 1000;
            var timer = Stopwatch.StartNew();
            while (marker < list.Count)
            {
                var take = list.Skip(marker).Take(1000).ToList();
                if (!take.Any())
                {
                    break;
                }

                var nodes = take.OfType<Node>().Select(x => new UpdateRquest { PartitionKey = x.Id, Data = x }).ToList();
                var edges = take.OfType<Edge>().Select(x => new UpdateRquest { PartitionKey = x.Id, Data = x }).ToList();
                reporter.Start();

                {
                    var rsp = await httpClient.PostAsync($"{url}/api/store/{instance}/twin", JsonContent.Create(nodes));
                    ProcessResponse(rsp);
                    Console.WriteLine($"{instance} Created {nodes.Count()} nodes");
                    reporter.AddNodes(instance, nodes.Count());
                }

                {
                    var rsp = await httpClient.PostAsync($"{url}/api/store/{instance}/relationship", JsonContent.Create(edges));
                    ProcessResponse(rsp);
                    Console.WriteLine($"{instance} Created {edges.Count()} edges");
                    reporter.AddEdges(instance, edges.Count());
                }

                Console.WriteLine($"Completed ingestion of {marker} entities in {timer.Elapsed.TotalSeconds} seconds.");
                marker += batchSize;
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