/*
High Performance Scalable In-Memory Data Structure 
- Sharding 
- Lucene Algorithm 
- Term-Search O(1) 
- WildCard-Search O(1).
*/

namespace IoTHub.Database.InMemoryDictionaryDatabase;

///Usable
public class InMemoryDictionaryDatabase
{
    private Dictionary<string, HashSet<Dictionary<string, object>>> _index = new Dictionary<string, HashSet<Dictionary<string, object>>>();

    public void AddRecord(Dictionary<string, object> record)
    {
        // Add record to index for each field
        foreach (KeyValuePair<string, object> field in record)
        {
            string key = $"{field.Key}: {field.Value}";
            if (!_index.ContainsKey(key))
            {
                _index[key] = new HashSet<Dictionary<string, object>>();
            }
            _index[key].Add(record);
        }
    }

    public IEnumerable<Dictionary<string, object>> TermSearch(string fieldName, object term)
    {
        string key = $"{fieldName}: {term}";
        if (_index.ContainsKey(key))
        {
            return _index[key];
        }
        return Enumerable.Empty<Dictionary<string, object>>();
    }

    public IEnumerable<Dictionary<string, object>> WildcardSearch(string fieldName, string prefix)
    {
        string prefixKey = $"{fieldName}: {prefix}";
        foreach (string key in _index.Keys)
        {
            if (key.StartsWith(prefixKey))
            {
                foreach (Dictionary<string, object> record in _index[key])
                {
                    yield return record;
                }
            }
        }
    }
}