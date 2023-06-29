/* Implement an High Performance Scalable In-Memory Data Structure in C#
which contains Single Table with Dynamic fields. in this structure Queries
should execute in milliseconds for billions of records, It should provide O(1) 
for term search and O(1) for wild-card search.
*/
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

public class InMemoryDataStructure
{
    private ConcurrentDictionary<string, ConcurrentDictionary<string, object>> table; // map from row IDs to rows
    private ConcurrentDictionary<string, ConcurrentDictionary<object, HashSet<string>>> invertedIndexes; // map from field names to inverted indexes
    private ConcurrentDictionary<string, BloomFilter> bloomFilters; // map from field names to bloom filters

    public InMemoryDataStructure()
    {
        table = new ConcurrentDictionary<string, ConcurrentDictionary<string, object>>();
        invertedIndexes = new ConcurrentDictionary<string, ConcurrentDictionary<object, HashSet<string>>>();
        bloomFilters = new ConcurrentDictionary<string, BloomFilter>();
    }

    public void AddRow(string id, Dictionary<string, object> fields)
    {
        ConcurrentDictionary<string, object> row = new ConcurrentDictionary<string, object>(fields);
        table[id] = row;

        foreach (var kvp in fields)
        {
            string fieldName = kvp.Key;
            object fieldValue = kvp.Value;

            if (!invertedIndexes.ContainsKey(fieldName))
            {
                invertedIndexes[fieldName] = new ConcurrentDictionary<object, HashSet<string>>();
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
            else
            {
                if (!invertedIndexes[fieldName].ContainsKey(fieldValue))
                {
                    invertedIndexes[fieldName][fieldValue] = new HashSet<string>();
                }

                invertedIndexes[fieldName][fieldValue].Add(id);
                bloomFilters[fieldName].Add(fieldValue);
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
            return invertedIndexes[fieldName][searchTerm].Select(id => table[id].ToDictionary(x => x.Key, x => x.Value)).ToList();
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

        List<object> matchingTerms = invertedIndexes[fieldName].Keys.Where(term => ((string)term).StartsWith(searchTerm)).ToList();

        List<Dictionary<string, object>> results = new List<Dictionary<string, object>>();

        foreach (object term in matchingTerms)
        {
            results.AddRange(invertedIndexes[fieldName][term].Select(id => table[id].ToDictionary(x => x.Key, x => x.Value)));
        }

        return results;
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

        public void Add(object value)
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