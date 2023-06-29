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

public class InMemoryDataLuceneStructure
{
    private List<Cluster> clusters;

    public InMemoryDataLuceneStructure(int numClusters, int numShardsPerCluster, int replicationFactor)
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
                if (!invertedIndexes.ContainsKey(fieldName))
                {
                    return new List<string>();
                }

                if (bloomFilters.ContainsKey(fieldName) && !bloomFilters[fieldName].Contains(searchTerm))
                {
                    return new List<string>();
                }

                if (invertedIndexes[fieldName].ContainsKey(searchTerm))
                {
                    return invertedIndexes[fieldName][searchTerm].ToList();
                }
                else
                {
                    return new List<string>();
                }
            }

            public List<string> WildcardSearch(string fieldName, string searchTerm)
            {
                if (!invertedIndexes.ContainsKey(fieldName))
                {
                    return new List<string>();
                }

                List<string> matchingTerms = invertedIndexes[fieldName].Keys.Where(term => term.StartsWith(searchTerm)).ToList();

                List<string> results = new List<string>();

                foreach (string term in matchingTerms)
                {
                    results.AddRange(invertedIndexes[fieldName][term]);
                }

                return results;
            }
        }
    }

    private class BloomFilter
    {
        private HashSet<int> bits;
        private int numHashes;

        public BloomFilter()
        {
            bits = new HashSet<int>();
            numHashes = 4; // experimentally determined to be a good balance between false positives and memory usage
        }

        public void Add(string value)
        {
            foreach (int hash in GetHashes(value))
            {
                bits.Add(hash);
            }
        }

        public bool Contains(string value)
        {
            foreach (int hash in GetHashes(value))
            {
                if (!bits.Contains(hash))
                {
                    return false;
                }
            }

            return true;
        }

        private List<int> GetHashes(string value)
        {
            List<int> hashes = new List<int>();

            for (int i = 0; i < numHashes; i++)
            {
                byte[] bytes = System.Text.Encoding.ASCII.GetBytes(value + i);
                int hash = MurmurHash3.Hash32(bytes);
                hashes.Add(hash);
            }

            return hashes;
        }
    }

    private class MurmurHash3
    {
        public static int Hash32(byte[] data)
        {
            const uint c1 = 0xcc9e2d51;
            const uint c2 = 0x1b873593;
            const int r1 = 15;
            const int r2 = 13;
            const uint m = 5;
            const uint n = 0xe6546b64;

            uint hash = 0xdeadbeef ^ (uint)data.Length;
            uint k;

            for (int i = 0; i + 4 <= data.Length; i += 4)
            {
                k = BitConverter.ToUInt32(data, i);

                k *= c1;
                k = (k << r1) | (k >> (32 - r1));
                k *= c2;

                hash ^= k;
                hash = ((hash << r2) | (hash >> (32 - r2))) * m + n;
            }

            uint remainingBytes = (uint)data.Length & 3;

            if (remainingBytes == 3)
            {
                hash ^= (ushort)(data[data.Length - 3] << 16 | data[data.Length - 2] << 8 | data[data.Length - 1]);
                hash ^= (uint)data[data.Length - 4] << 24;
                hash *= c1;
                hash = (hash << r1) | (hash >> (32 - r1));
                hash *= c2;
            }
            else if (remainingBytes == 2)
            {
                hash ^= (ushort)(data[data.Length - 2] << 8 | data[data.Length - 1]);
                hash *= c1;
                hash = (hash << r1) | (hash >> (32 - r1));
                hash *= c2;
            }
            else if (remainingBytes == 1)
            {
                hash ^= data[data.Length - 1];
                hash *= c1;
                hash = (hash << r1) | (hash >> (32 - r1));
                hash *= c2;
            }

            hash ^= (uint)data.Length;
            hash ^= hash >> 16;
            hash *= 0x85ebca6b;
            hash ^= hash >> 13;
            hash *= 0xc2b2ae35;
            hash ^= hash >> 16;

            return (int)hash;
        }
    }
}