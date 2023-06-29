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

public class InMemoryClusteringDataStructure
{
    private Dictionary<string, Dictionary<string, object>> documents; // map from document ids to documents
    private Dictionary<string, Dictionary<string, HashSet<string>>> invertedIndexes; // map from field names to inverted indexes
    private Dictionary<string, BloomFilter> bloomFilters; // map from field names to bloom filters

    public InMemoryClusteringDataStructure()
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
            // add additional data structure types for other data types
        }
    }

    public List<string> Search(string fieldName, string searchTerm)
    {
        if (!invertedIndexes.ContainsKey(fieldName))
        {
            return new List<string>();
        }

        if (!bloomFilters[fieldName].Contains(searchTerm))
        {
            return new List<string>();
        }

        return new List<string>(invertedIndexes[fieldName][searchTerm]);
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
            if (kvp.Key.Contains(searchTerm))
            {
                results.AddRange(kvp.Value);
            }
        }

        return results;
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
