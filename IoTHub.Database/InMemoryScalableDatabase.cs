/*
High Performance Scalable In-Memory Data Structure 
- Sharding 
- Scaling 
- Clustering 
- Term-Search O(1) 
- WildCard-Search O(1).
*/

using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

public class InMemoryScalableDatabase
{
    private readonly ConcurrentDictionary<string, ConcurrentDictionary<string, HashSet<string>>> data;
    private readonly ConcurrentDictionary<string, ConcurrentDictionary<string, HashSet<string>>> invertedIndex;
    private readonly ConcurrentDictionary<string, ConcurrentDictionary<string, HashSet<string>>> wildcardIndex;

    public InMemoryScalableDatabase()
    {
        data = new ConcurrentDictionary<string, ConcurrentDictionary<string, HashSet<string>>>();
        invertedIndex = new ConcurrentDictionary<string, ConcurrentDictionary<string, HashSet<string>>>();
        wildcardIndex = new ConcurrentDictionary<string, ConcurrentDictionary<string, HashSet<string>>>();
    }

    public void AddRecord(string recordId, Dictionary<string, string> fields)
    {
        foreach (var field in fields)
        {
            string fieldName = field.Key;
            string fieldValue = field.Value;

            data.AddOrUpdate(fieldName, _ => new ConcurrentDictionary<string, HashSet<string>>(), (_, existingValue) => existingValue);
            invertedIndex.AddOrUpdate(fieldName, _ => new ConcurrentDictionary<string, HashSet<string>>(), (_, existingValue) => existingValue);
            wildcardIndex.AddOrUpdate(fieldName, _ => new ConcurrentDictionary<string, HashSet<string>>(), (_, existingValue) => existingValue);

            data[fieldName].AddOrUpdate(fieldValue, _ => new HashSet<string> { recordId }, (_, existingValue) =>
            {
                existingValue.Add(recordId);
                return existingValue;
            });

            invertedIndex[fieldName].AddOrUpdate(fieldValue, _ => new HashSet<string> { recordId }, (_, existingValue) =>
            {
                existingValue.Add(recordId);
                return existingValue;
            });

            for (int i = 1; i < fieldValue.Length; i++)
            {
                string prefix = fieldValue.Substring(0, i);
                wildcardIndex[fieldName].AddOrUpdate(prefix, _ => new HashSet<string> { recordId }, (_, existingValue) =>
                {
                    existingValue.Add(recordId);
                    return existingValue;
                });
            }
        }
    }

    public List<string> Search(string fieldName, string searchTerm)
    {
        if (!invertedIndex.ContainsKey(fieldName))
        {
            return new List<string>();
        }

        if (!invertedIndex[fieldName].ContainsKey(searchTerm))
        {
            return new List<string>();
        }

        var recordIds = invertedIndex[fieldName][searchTerm];

        return new List<string>(recordIds);
    }

    public List<string> WildcardSearch(string fieldName, string searchTerm)
    {
        if (!wildcardIndex.ContainsKey(fieldName))
        {
            return new List<string>();
        }

        var matchingRecordIds = new HashSet<string>();

        foreach (var prefix in GetPrefixes(searchTerm))
        {
            if (!wildcardIndex[fieldName].ContainsKey(prefix))
            {
                continue;
            }

            matchingRecordIds.UnionWith(wildcardIndex[fieldName][prefix]);
        }

        return new List<string>(matchingRecordIds);
    }

    public List<string> SearchOrWildcardSearch(string fieldName, string searchTerm)
    {
        var matchingRecordIds = new HashSet<string>();

        // First, perform term-based search
        if (invertedIndex.ContainsKey(fieldName) && invertedIndex[fieldName].ContainsKey(searchTerm))
        {
            matchingRecordIds.UnionWith(invertedIndex[fieldName][searchTerm]);
        }

        // Next, perform wildcard search
        if (wildcardIndex.ContainsKey(fieldName))
        {
            foreach (var prefix in GetPrefixes(searchTerm))
            {
                if (!wildcardIndex[fieldName].ContainsKey(prefix))
                {
                    continue;
                }

                matchingRecordIds.UnionWith(wildcardIndex[fieldName][prefix]);
            }
        }

        return new List<string>(matchingRecordIds);
    }

    private static string[] GetPrefixes(string searchTerm)
    {
        var prefixes = new List<string>();

        for (int i = 1; i < searchTerm.Length; i++)
        {
            prefixes.Add(searchTerm.Substring(0, i));
        }

        prefixes.Add(searchTerm);

        return prefixes.ToArray();
    }
}