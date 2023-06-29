/*
High Performance Scalable In-Memory Data Structure 
- Inverted Index
- Term-Search O(1) 
- WildCard-Search O(1).
*/

using System.Collections.Concurrent;

public class InMemoryInvertedIndexDatabase
{
    private readonly ConcurrentDictionary<string, HashSet<string>> data;
    private readonly ConcurrentDictionary<string, HashSet<string>> invertedIndex;
    private readonly ConcurrentDictionary<string, HashSet<string>> wildcardIndex;

    public InMemoryInvertedIndexDatabase()
    {
        data = new ConcurrentDictionary<string, HashSet<string>>();
        invertedIndex = new ConcurrentDictionary<string, HashSet<string>>();
        wildcardIndex = new ConcurrentDictionary<string, HashSet<string>>();
    }

    public void AddRecord(string recordId, Dictionary<string, string> fields)
    {
        foreach (var field in fields)
        {
            string fieldName = field.Key;
            string fieldValue = field.Value;

            data.AddOrUpdate(fieldName, new HashSet<string> { fieldValue }, (_, existingValue) =>
            {
                existingValue.Add(fieldValue);
                return existingValue;
            });

            string invertedIndexKey = $"{fieldName}:{fieldValue}";
            invertedIndex.AddOrUpdate(invertedIndexKey, new HashSet<string> { recordId }, (_, existingValue) =>
            {
                existingValue.Add(recordId);
                return existingValue;
            });

            for (int i = 1; i < fieldValue.Length; i++)
            {
                string prefix = fieldValue.Substring(0, i);
                string wildcardIndexKey = $"{fieldName}:{prefix}*";
                wildcardIndex.AddOrUpdate(wildcardIndexKey, new HashSet<string> { recordId }, (_, existingValue) =>
                {
                    existingValue.Add(recordId);
                    return existingValue;
                });
            }
        }
    }

    public List<string> Search(string fieldName, string searchTerm)
    {
        string invertedIndexKey = $"{fieldName}:{searchTerm}";
        if (!invertedIndex.ContainsKey(invertedIndexKey))
        {
            return new List<string>();
        }

        var recordIds = invertedIndex[invertedIndexKey];

        return new List<string>(recordIds);
    }

    public List<string> WildcardSearch(string fieldName, string searchTerm)
    {
        if (!data.ContainsKey(fieldName))
        {
            return new List<string>();
        }

        string wildcardIndexKey = $"{fieldName}:{searchTerm}*";
        if (!wildcardIndex.ContainsKey(wildcardIndexKey))
        {
            return new List<string>();
        }

        var recordIds = wildcardIndex[wildcardIndexKey];

        return new List<string>(recordIds);
    }

    public List<string> SearchOrWildcardSearch(string fieldName, string searchTerm)
    {
        var matchingRecordIds = new HashSet<string>();

        // First, perform term-based search
        string invertedIndexKey = $"{fieldName}:{searchTerm}";
        if (invertedIndex.ContainsKey(invertedIndexKey))
        {
            matchingRecordIds.UnionWith(invertedIndex[invertedIndexKey]);
        }

        // Next, perform wildcard search
        if (data.ContainsKey(fieldName))
        {
            string wildcardIndexKey = $"{fieldName}:{searchTerm}*";
            if (wildcardIndex.ContainsKey(wildcardIndexKey))
            {
                matchingRecordIds.UnionWith(wildcardIndex[wildcardIndexKey]);
            }
        }

        return new List<string>(matchingRecordIds);
    }
}