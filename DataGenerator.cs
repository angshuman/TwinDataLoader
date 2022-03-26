namespace TwinDataLoader
{
    using System;
    using System.Collections.Generic;
    using System.Linq;

    public class GraphEntity
    {
        public string Id { get; set; }

        public string Label { get; set; }

        public Dictionary<string, object> Properties { get; set; } = new Dictionary<string, object>();

        private readonly Random rand = new Random();

        public GraphEntity Update()
        {
            var newProperties = new Dictionary<string, object>();

            foreach (var prop in Properties)
            {
                if (prop.Value is string)
                {
                    newProperties[prop.Key] = Guid.NewGuid().ToString("D");
                }
                else if (prop.Value is int)
                {
                    newProperties[prop.Key] = rand.Next(50, 100);
                }
            }

            this.Properties = newProperties;
            return this;
        }
    }

    public class Node : GraphEntity
    {
        public string PartitionId { get; set; }
    }

    public class Edge : GraphEntity
    {
        public string EdgeType { get; set; }

        public string FromId { get; set; }

        public string ToId { get; set; }
    }

    public class DataGenerator
    {
        private readonly int level;
        private readonly int factor;
        private readonly string realationshipName;
        private readonly int numTwinProperties;
        private readonly int numEdgeProperties;
        private readonly bool generateReverseEdge;
        private readonly Random rand;
        private List<GraphEntity> entities = new List<GraphEntity>();

        private int nodeCount = 1;
        private int edgeCount = 0;

        public DataGenerator(int level,
            int factor,
            string relationshipName,
            int numTwinProperties,
            int numEdgeProperties,
            bool generateReverseEdge)
        {
            this.level = level;
            this.factor = factor;
            this.realationshipName = relationshipName;
            this.numTwinProperties = numTwinProperties;
            this.numEdgeProperties = numEdgeProperties;
            this.generateReverseEdge = generateReverseEdge;
            rand = new Random();
        }

        public IEnumerable<GraphEntity> Generate()
        {
            var roots = new List<Node> { GetNode("0", "twin", 0) };
            entities.AddRange(roots);
            var rest = GenerateLevel(roots, 1);
            entities.AddRange(rest);
            return entities;
        }

        public IEnumerable<GraphEntity> Update()
        {
            entities = entities.Select(x => x.Update()).ToList();
            return entities;
        }

        public Dictionary<string, int> GetCount()
        {
            return new Dictionary<string, int> {
                { "total", entities.Count() },
                { "nodes", entities.Where(x => x is Node).Count() },
                { "edges", entities.Where(x=> x is Edge).Count() }
            };
        }

        private IEnumerable<GraphEntity> GenerateLevel(List<Node> previousNodes, int currentLevel)
        {
            if (currentLevel == level)
            {
                yield break;
            }

            var levelNodes = new List<Node>();

            foreach (var previousNode in previousNodes)
            {
                for (var count = 0; count < factor; count++)
                {
                    var newNode = GetNode(id: nodeCount++.ToString(), label: "twin", level: currentLevel);
                    levelNodes.Add(newNode);
                    yield return newNode;

                    var edges = GetEdge(edgeCount++.ToString(), this.realationshipName, previousNode.Id, newNode.Id, this.generateReverseEdge);

                    foreach (var edge in edges)
                    {
                        yield return edge;
                    }
                }
            }

            Node previousSibling = null;
            foreach (var item in levelNodes)
            {
                if (previousSibling == null)
                {
                    previousSibling = item;
                    continue;
                }

                var edges = GetEdge(edgeCount++.ToString(), $"{this.realationshipName}-next-{level}", previousSibling.Id, item.Id, this.generateReverseEdge);
                previousSibling = item;
                foreach (var edge in edges)
                {
                    yield return edge;
                }
            }

            foreach (var item in GenerateLevel(levelNodes, currentLevel + 1))
            {
                yield return item;
            }
        }

        private Node GetNode(string id, string label, int level)
        {
            var newNode = new Node
            {
                Id = id,
                Label = label,
            };

            newNode.PartitionId = newNode.Id;
            newNode.Properties.Add("temperature", rand.Next(50, 100));
            newNode.Properties.Add("humidity", rand.Next(50, 100));
            newNode.Properties.Add("pressure", rand.Next(50, 100));
            newNode.Properties.Add("level", level);
            newNode.Properties.Add("coordinates", new int[] { 0, rand.Next(), rand.Next(), rand.Next(), level });
            newNode.Properties.Add($"dyn-{level}", Guid.NewGuid().ToString("D"));
            newNode.Properties.Add($"guid-{Guid.NewGuid():D}", "dynProp");

            for (var i = 0; i < numTwinProperties; i++)
            {
                var propName = $"prop{i:D3}";
                newNode.Properties[propName] = rand.Next(0, 100);
            }

            return newNode;
        }

        private IEnumerable<Edge> GetEdge(string id, string label, string from, string to, bool generateReverse)
        {
            var properties = new Dictionary<string, object> { { "length", rand.Next(10) } };
            for (var i = 0; i < numEdgeProperties; i++)
            {
                var propName = $"prop{i.ToString("D3")}";
                properties[propName] = rand.Next(0, 100);
            }

            yield return new Edge
            {
                Id = id,
                Label = label,
                EdgeType = "Outgoing",
                FromId = from,
                ToId = to,
                Properties = properties,
            };

            if (generateReverse)
            {
                yield return new Edge
                {
                    Id = $"re:{id}",
                    Label = label,
                    EdgeType = "Reverse",
                    FromId = to,
                    ToId = from,
                    Properties = properties,
                };
            }
        }
    }
}

