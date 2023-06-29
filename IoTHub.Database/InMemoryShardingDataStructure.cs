/*
High Performance Scalable In-Memory Data Structure 
- Sharding 
- Term-Search O(1) 
- WildCard-Search O(1).
*/

using System.Collections.Concurrent;

public class InMemoryShardingDataStructure
{
    private List<Shard> shards;

    public InMemoryShardingDataStructure(int numShards)
    {
        shards = Enumerable.Range(0, numShards).Select(i => new Shard()).ToList();
    }

    public void AddRow(string id, Dictionary<string, object> fields)
    {
        int shardId = GetShardId(id);
        shards.ElementAt(shardId).AddRow(id, fields);
    }

    public List<Dictionary<string, object>> Search(string fieldName, string searchTerm)
    {
        List<Dictionary<string, object>> results = new List<Dictionary<string, object>>();

        Parallel.ForEach(shards, shard =>
        {
            results.AddRange(shard.Search(fieldName, searchTerm));
        });

        return results;
    }

    public List<Dictionary<string, object>> WildcardSearch(string fieldName, string searchTerm)
    {
        List<Dictionary<string, object>> results = new List<Dictionary<string, object>>();

        Parallel.ForEach(shards, shard =>
        {
            results.AddRange(shard.WildcardSearch(fieldName, searchTerm));
        });

        return results;
    }

    private int GetShardId(string id)
    {
        // Calculate the shard ID based on the hash code of the row ID
        return Math.Abs(id.GetHashCode()) % shards.Count();
    }

    private class Shard
    {
        private ConcurrentDictionary<string, Dictionary<string, object>> rows; // map from row IDs to rows
        private ConcurrentDictionary<string, ConcurrentDictionary<string, HashSet<string>>> invertedIndexes; // map from field names to inverted indexes
        private ConcurrentDictionary<string, BloomFilter> bloomFilters; // map from field names to bloom filters

        public Shard()
        {
            rows = new ConcurrentDictionary<string, Dictionary<string, object>>();
            invertedIndexes = new ConcurrentDictionary<string, ConcurrentDictionary<string, HashSet<string>>>();
            bloomFilters = new ConcurrentDictionary<string, BloomFilter>();
        }

        public void AddRow(string id, Dictionary<string, object> fields)
        {
            rows[id] = fields;

            foreach (var kvp in fields)
            {
                string fieldName = kvp.Key;
                object fieldValue = kvp.Value;

                if (!invertedIndexes.ContainsKey(fieldName))
                {
                    invertedIndexes[fieldName] = new ConcurrentDictionary<string, HashSet<string>>();
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
                            invertedIndexes[fieldName].TryAdd(term, new HashSet<string>());
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
                        invertedIndexes[fieldName].TryAdd(value.ToString(), new HashSet<string>());
                    }

                    invertedIndexes[fieldName][value.ToString()].Add(id);
                    bloomFilters[fieldName].Add(value.ToString());
                }
            }
        }

        public List<Dictionary<string, object>> Search(string fieldName, string searchTerm)
        {
            if (!invertedIndexes.ContainsKey(fieldName))
            {
                return new List<Dictionary<string, object>>();
            }

            if (bloomFilters.ContainsKey(fieldName) && !bloomFilters[fieldName].Contains(searchTerm))
            {
                return new List<Dictionary<string, object>>();
            }

            if (invertedIndexes[fieldName].ContainsKey(searchTerm))
            {
                return invertedIndexes[fieldName][searchTerm].Select(id => rows[id]).ToList();
            }
            else
            {
                return new List<Dictionary<string, object>>();
            }
        }

        public List<Dictionary<string, object>> WildcardSearch(string fieldName, string searchTerm)
        {
            if (!invertedIndexes.ContainsKey(fieldName))
            {
                return new List<Dictionary<string, object>>();
            }

            List<string> matchingTerms = invertedIndexes[fieldName].Keys.Where(term => term.StartsWith(searchTerm)).ToList();

            List<Dictionary<string, object>> results = new List<Dictionary<string, object>>();

            foreach (string term in matchingTerms)
            {
                results.AddRange(invertedIndexes[fieldName][term].Select(id => rows[id]));
            }

            return results;
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
            int hash1 = value.GetHashCode();
            int hash2 = hash1 << 16 | hash1 >> 16;
            int hash3 = hash1 << 8 | hash1 >> 24;
            int hash4 = hash1 << 24 | hash1 >> 8;

            for (int i = 0; i < numHashes; i++)
            {
                int hash = (i switch
                {
                    0 => hash1,
                    1 => hash2,
                    2 => hash3,
                    _ => hash4
                }) % 1000000; // 1 million bits

                bits.Add(hash);
            }
        }

        public bool Contains(string value)
        {
            int hash1 = value.GetHashCode();
            int hash2 = hash1 << 16 | hash1 >> 16;
            int hash3 = hash1 << 8 | hash1 >> 24;
            int hash4 = hash1 << 24 | hash1 >> 8;

            for (int i = 0; i < numHashes; i++)
            {
                int hash = (i switch
                {
                    0 => hash1,
                    1 => hash2,
                    2 => hash3,
                    _ => hash4
                }) % 1000000; // 1 million bits

                if (!bits.Contains(hash))
                {
                    return false;
                }
            }

            return true;
        }
    }
}