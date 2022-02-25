namespace TwinDataLoader
{
    using System;
    using System.Collections.Concurrent;
    using System.Collections.Generic;
    using System.Diagnostics;
    using System.Text;
    using System.Threading;
    using System.Threading.Tasks;
    using Neo4j.Driver;

    public class Neo4JIndexer : IDisposable
    {
        private readonly string url;
        private readonly string instance;
        private readonly Neo4JIndexerReporter reporter;
        private readonly IDriver driver;

        public Neo4JIndexer(string url, string user, string pass, string instance, Neo4JIndexerReporter reporter)
        {
            this.url = $"{url}/{instance}";
            this.instance = instance;
            this.reporter = reporter;
            driver = GraphDatabase.Driver(this.url, AuthTokens.Basic(user, pass));
            Console.WriteLine($"{nameof(Neo4JIndexer)}: Indexer for {this.url}");
        }

        public void Dispose()
        {
            driver.Dispose();
        }

        public async Task IngestAsync(IEnumerable<object> data)
        {

            using var session = driver.AsyncSession();

            try
            {
                await session.RunAsync($"CREATE DATABASE {this.instance} IF NOT EXISTS");
                await session.RunAsync("CREATE CONSTRAINT ON (n:twin) ASSERT n.id IS UNIQUE IF NOT EXISTS");
                // await session.RunAsync("CREATE CONSTRAINT ON [e:rel] ASSERT e.id IS UNIQUE");
            }
            catch (Exception e)
            {
                Console.WriteLine(e.Message);
            }

            foreach (var item in data)
            {
                try
                {
                    if (item is Node node)
                    {
                        var (createStatement, values) = CreateNeo4JNode(node);
                        await session.RunAsync(createStatement, values);
                        reporter.AddNodes(instance, 1);

                    }
                    else if (item is Edge edge)
                    {
                        var (createStatement, values) = CreateNeo4JEdge(edge);
                        await session.RunAsync(createStatement, values);
                        reporter.AddEdges(instance, 1);
                    }
                }
                catch (Exception e)
                {
                    Console.WriteLine($"{nameof(Neo4JIndexer)}: Error in ingestion. {e.Message}");
                    reporter.ErrorCount++;
                }
            }
        }

        public (string, Dictionary<string, object>) CreateNeo4JNode(Node node)
        {
            var createStatement = new StringBuilder();
            createStatement.Append($"MERGE (n {{id: '{node.Id}'}}) ")
                .Append("SET n.label = $label ")
                .Append("SET n.partitionId = $partitionId ");

            var values = new Dictionary<string, object>
            {
                { "label", node.Label},
                { "partitionId", node.PartitionId }
            };

            foreach (var prop in node.Properties)
            {
                createStatement.Append($"SET n.{prop.Key} = ${prop.Key} ");
                values.Add(prop.Key, prop.Value);
            }

            return (createStatement.ToString(), values);
        }

        public (string, Dictionary<string, object>) CreateNeo4JEdge(Edge edge)
        {
            var createStatement = new StringBuilder();
            createStatement.Append("MATCH (from), (to) ")
                 .Append("WHERE from.id = $fromId AND to.id = $toId ")
                 .Append($"CREATE (from)-[e:rel]->(to) ")
                 .Append("SET e.id = $id ")
                 .Append("SET e.label = $label ");

            /*
            createStatement.Append("MATCH (from) WHERE id(from) = $fromId ")
                .Append("MATCH (to) WHERE id(to) = $toId ")
                .Append($"CREATE (from)-[e:rel {{id: '{edge.Id}'}}]->(to) ");
            */

            /*
            createStatement.Append("MATCH (from) WHERE id(from) = $fromId ")
                .Append("MATCH (to) WHERE id(to) = $toId ")
                .Append($"MERGE (from)-[e {{id: '{edge.Id}'}}]->(to) ");
            */

            /*
            createStatement.Append($"MERGE (from {{ id: '{edge.FromId}'}})-[e {{id : '{edge.Id}'}}]->(to {{ id: '{edge.ToId}'}}) ")
                // .Append("ON CREATE SET (from)-[e]-(to) ")
                .Append("ON CREATE SET e.label = $label ");
            */


            /* 
             * // Does not work
             * createStatement.Append("MATCH (from), (to) ")
             *   .Append("WHERE from.id = $fromId AND to.id = $toId ")
             *   .Append($"MERGE (from)-[e {{id: '{edge.Id}'}}]->(to) ")
             *   .Append("SET e.label = $label ");
             */

            var values = new Dictionary<string, object>
            {
                { "id", edge.Id },
                { "fromId", edge.FromId },
                { "toId", edge.ToId },
                { "label", edge.Label }
            };

            foreach (var prop in edge.Properties)
            {
                createStatement.Append($"SET e.{prop.Key} = ${prop.Key} ");
                values.Add(prop.Key, prop.Value);
            }

            // createStatement.Append("WHERE from.id = $fromId AND to.id = $toId ");
            return (createStatement.ToString(), values);
        }
    }

    public class Neo4JIndexerReporter
    {
        private static readonly Stopwatch watch = Stopwatch.StartNew();
        private static readonly ConcurrentDictionary<string, Int32> counts = new ConcurrentDictionary<string, Int32>();
        private readonly Timer timer = new Timer(TimedReportCallback, null, TimeSpan.FromSeconds(1), TimeSpan.FromSeconds(1));

        public int ErrorCount { get; set; }

        private static void TimedReportCallback(object state)
        {
            var total = 0;
            foreach (var item in counts)
            {
                Console.WriteLine($"{item.Key} {item.Value}");
                total += item.Value;
            }

            Console.WriteLine($"{nameof(Neo4JIndexerReporter)}: Entities added {total} in {watch.Elapsed.TotalSeconds} seconds");
            Console.WriteLine($"{nameof(Neo4JIndexerReporter)}: Rate added {total / watch.Elapsed.TotalSeconds} items/second");
        }

        public void Start()
        {
            watch.Start();
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

            Console.WriteLine($"{nameof(Neo4JIndexerReporter)}: Entities added {total} in {watch.Elapsed.TotalSeconds} seconds");
            Console.WriteLine($"{nameof(Neo4JIndexerReporter)}: Rate added {total / watch.Elapsed.TotalSeconds} items/second");
            Console.WriteLine($"{nameof(Neo4JIndexerReporter)}: ErrorCount: {ErrorCount}");
        }
    }
}