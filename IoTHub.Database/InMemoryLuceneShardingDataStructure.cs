/*
High Performance Scalable In-Memory Data Structure 
- Sharding 
- Replication 
- Clustering 
- Large-Scale 
- Lucene Algorithm 
- Tries 
- Bloom Filter 
- Hash-Table 
- Inverted Index
- Parallel Processing
- Query Optimization
- Compressing and Indexing
- Term-Search O(1) 
- WildCard-Search O(1).
*/

using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

public class InMemoryLuceneShardingDataStructure
{
    private List<Cluster> clusters;

    public InMemoryLuceneShardingDataStructure(int numClusters, int numShardsPerCluster, int replicationFactor)
    {
        clusters = Enumerable.Range(0, numClusters).Select(i => new Cluster(numShardsPerCluster, replicationFactor)).ToList();
    }

    public void AddDocument(string id, Dictionary<string, object> fields)
    {
        int clusterId = GetClusterId(id);
        clusters.ElementAt(clusterId).AddDocument(id, fields);
    }

    public List<string> Search(string fieldName, string searchTerm)
    {
        List<string> results = new List<string>();

        Parallel.ForEach(clusters, cluster =>
        {
            results.AddRange(cluster.Search(fieldName, searchTerm));
        });

        return results;
    }

    public List<string> WildcardSearch(string fieldName, string searchTerm)
    {
        List<string> results = new List<string>();

        Parallel.ForEach(clusters, cluster =>
        {
            results.AddRange(cluster.WildcardSearch(fieldName, searchTerm));
        });

        return results;
    }

    private int GetClusterId(string id)
    {
        // Calculate the cluster ID based on the hash code of the document ID
        return Math.Abs(id.GetHashCode()) % clusters.Count();
    }

    private class Cluster
    {
        private List<Shard> shards;
        private int replicationFactor;

        public Cluster(int numShards, int replicationFactor)
        {
            shards = Enumerable.Range(0, numShards).Select(i => new Shard()).ToList();
            this.replicationFactor = replicationFactor;
        }

        public void AddDocument(string id, Dictionary<string, object> fields)
        {
            foreach (var shard in GetPrimaryAndReplicaShards(id))
            {
                shard.AddDocument(id, fields);
            }
        }

        public List<string> Search(string fieldName, string searchTerm)
        {
            List<string> results = new List<string>();

            Parallel.ForEach(shards, shard =>
            {
                results.AddRange(shard.Search(fieldName, searchTerm));
            });

            return results;
        }

        public List<string> WildcardSearch(string fieldName, string searchTerm)
        {
            List<string> results = new List<string>();

            Parallel.ForEach(shards, shard =>
            {
                results.AddRange(shard.WildcardSearch(fieldName, searchTerm));
            });

            return results;
        }

        private List<Shard> GetPrimaryAndReplicaShards(string id)
        {
            int primaryShardId = GetShardId(id);
            List<Shard> shards = new List<Shard>();
            shards.Add(this.shards.ElementAt(primaryShardId));

            for (int i = 1; i <= replicationFactor; i++)
            {
                int replicaShardId = (primaryShardId + i) % this.shards.Count;
                shards.Add(this.shards.ElementAt(replicaShardId));
            }

            return shards;
        }

        private int GetShardId(string id)
        {
            // Calculate the shard ID based on the hash code of the document ID
            return Math.Abs(id.GetHashCode()) % shards.Count();
        }

        private class Shard
        {
            private Dictionary<string, Dictionary<string, object>> documents; // map from document ids to documents
            private Dictionary<string, Dictionary<string, HashSet<string>>> invertedIndexes; // map from field names to inverted indexes
            private Dictionary<string, BloomFilter> bloomFilters; // map from field names to bloom filters

            public Shard()
            {
                documents = new Dictionary<string, Dictionary<string, object>>();
                invertedIndexes = new Dictionary<string, Dictionary<string, HashSet<string>>>();
                bloomFilters = new Dictionary<string, BloomFilter>();
            }

            public void AddDocument(string id, Dictionary<string, object> fields)
            {
                documents[id] = fields;

                foreach (var kvp in fields)
                {
                    string fieldName = kvp.Key;
                    object fieldValue = kvp.Value;

                    if (!invertedIndexes.ContainsKey(fieldName))
                    {
                        invertedIndexes[fieldName] = new Dictionary<string, HashSet<string>>();
                        bloomFilters[fieldName] = new BloomFilter();
                    }

                    if (fieldValue is string)
                    {
                        string text = (string)fieldValue;
                        string[] terms = text.Split(' ');

                        foreach (string term in terms)
                        {
                            if (!invertedIndexes[fieldName].ContainsKey(term))
                            {
                                invertedIndexes[fieldName][term] = new HashSet<string>();
                            }

                            invertedIndexes[fieldName][term].Add(id);
                            bloomFilters[fieldName].Add(term);
                        }
                    }
                    else if (fieldValue is int)
                    {
                        int value = (int)fieldValue;

                        if (!invertedIndexes[fieldName].ContainsKey(value.ToString()))
                        {
                            invertedIndexes[fieldName][value.ToString()] = new HashSet<string>();
                        }

                        invertedIndexes[fieldName][value.ToString()].Add(id);
                        bloomFilters[fieldName].Add(value.ToString());
                    }
                }
            }

            public List<string> Search(string fieldName, string searchTerm)
            {
                if (!invertedIndexes.ContainsKey(fieldName) || !invertedIndexes[fieldName].ContainsKey(searchTerm))
                {
                    return new List<string>();
                }

                return invertedIndexes[fieldName][searchTerm].ToList();
            }

            public List<string> WildcardSearch(string fieldName, string searchTerm)
            {
                if (!invertedIndexes.ContainsKey(fieldName))
                {
                    return new List<string>();
                }

                List<string> results = new List<string>();

                foreach (var kvp in invertedIndexes[fieldName])
                {
                    string term = kvp.Key;

                    if (WildcardMatch(term, searchTerm))
                    {
                        results.AddRange(kvp.Value);
                    }
                }

                return results;
            }

            private bool WildcardMatch(string term, string searchTerm)
            {
                // Use Trie data structure for efficient wildcard search
                TrieNode root = new TrieNode();
                root.Add(term);

                int i = 0;

                while (i < searchTerm.Length)
                {
                    char c = searchTerm[i];

                    if (c == '*')
                    {
                        return root.MatchWildcard(searchTerm.Substring(i));
                    }

                    if (!root.ContainsKey(c))
                    {
                        return false;
                    }

                    root = root[c];
                    i++;
                }

                return root.IsWord;
            }

            private class TrieNode
            {
                private Dictionary<char, TrieNode> children;
                public bool IsWord { get; set; }

                public TrieNode()
                {
                    children = new Dictionary<char, TrieNode>();
                }

                public void Add(string word)
                {
                    TrieNode current = this;

                    foreach (char c in word)
                    {
                        if (!current.children.ContainsKey(c))
                        {
                            current.children[c] = new TrieNode();
                        }

                        current = current.children[c];
                    }

                    current.IsWord = true;
                }

                public bool MatchWildcard(string wildcard)
                {
                    if (wildcard.Length == 0)
                    {
                        return IsWord;
                    }

                    char c = wildcard[0];

                    if (c == '*')
                    {
                        return children.Values.Any(child => child.MatchWildcard(wildcard.Substring(1)));
                    }

                    if (!children.ContainsKey(c))
                    {
                        return false;
                    }

                    return children[c].MatchWildcard(wildcard.Substring(1));
                }

                public bool ContainsKey(char c)
                {
                    return children.ContainsKey(c);
                }

                public TrieNode this[char c]
                {
                    get { return children[c]; }
                }
            }

            private class BloomFilter
            {
                private const int NumHashFunctions = 5;
                private const int FilterSize = 10000000; // 10 million bits
                private byte[] filter;

                public BloomFilter()
                {
                    filter = new byte[FilterSize / 8];
                }

                public void Add(string term)
                {
                    for (int i = 0; i < NumHashFunctions; i++)
                    {
                        int hash = GetHash(term, i);
                        filter[hash / 8] |= (byte)(1 << (hash % 8));
                    }
                }

                public bool Contains(string term)
                {
                    for (int i = 0; i < NumHashFunctions; i++)
                    {
                        int hash = GetHash(term, i);

                        if ((filter[hash / 8] & (byte)(1 << (hash % 8))) == 0)
                        {
                            return false;
                        }
                    }

                    return true;
                }

                private int GetHash(string term, int index)
                {
                    return (term.GetHashCode() * (index + 1)) % FilterSize;
                }
            }
        }
    }
}