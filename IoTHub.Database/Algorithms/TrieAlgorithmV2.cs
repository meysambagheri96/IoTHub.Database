namespace TrieAlgorithmV2;

using System;
using System.Collections.Generic;
using System.Linq;

public class Record
{
    public string Id { get; set; }
    public Dictionary<string, string> Fields { get; set; }
}

public class TrieNode
{
    private readonly Dictionary<char, TrieNode> children = new Dictionary<char, TrieNode>();
    public HashSet<string> RecordIds { get; } = new HashSet<string>();

    public void Add(string word, string recordId)
    {
        var node = this;
        foreach (var c in word)
        {
            if (!node.children.TryGetValue(c, out var child))
            {
                child = new TrieNode();
                node.children[c] = child;
            }
            node = child;
        }
        node.RecordIds.Add(recordId);
    }

    public HashSet<string> Search(string word)
    {
        var node = this;
        foreach (var c in word)
        {
            if (!node.children.TryGetValue(c, out node))
            {
                return new HashSet<string>();
            }
        }
        return node.RecordIds;
    }

    public HashSet<string> SearchWildcard(string pattern)
    {
        var matchingRecordIds = new HashSet<string>();
        var queue = new Queue<(TrieNode, int)>();
        queue.Enqueue((this, 0));

        while (queue.Count > 0)
        {
            var (node, index) = queue.Dequeue();
            if (index == pattern.Length)
            {
                matchingRecordIds.UnionWith(node.RecordIds);
            }
            else
            {
                var c = pattern[index];
                if (c == '*')
                {
                    foreach (var child in node.children.Values)
                    {
                        queue.Enqueue((child, index));
                    }
                }
                else if (node.children.TryGetValue(c, out var child))
                {
                    queue.Enqueue((child, index + 1));
                }
            }
        }

        return matchingRecordIds;
    }
}

public class FieldIndex
{
    private readonly TrieNode trie = new TrieNode();

    public void AddTerm(string term, string recordId)
    {
        trie.Add(term, recordId);
    }

    public IEnumerable<string> SearchTerm(string term)
    {
        return trie.Search(term);
    }

    public IEnumerable<string> SearchWildcard(string wildcard)
    {
        return trie.SearchWildcard(wildcard);
    }
}

public class Database
{
    private readonly Dictionary<string, FieldIndex> fieldIndexes = new Dictionary<string, FieldIndex>();
    private readonly Dictionary<string, Record> records = new Dictionary<string, Record>();

    public void AddRecord(Record record)
    {
        records[record.Id] = record;

        foreach (var field in record.Fields)
        {
            if (!fieldIndexes.TryGetValue(field.Key, out var fieldIndex))
            {
                fieldIndex = new FieldIndex();
                fieldIndexes[field.Key] = fieldIndex;
            }

            var words = field.Value.Split(' ');
            foreach (var word in words)
            {
                fieldIndex.AddTerm(word, record.Id);
            }
        }
    }

    public IEnumerable<Record> Search(string fieldName, string fieldValue)
    {
        if (fieldIndexes.TryGetValue(fieldName, out var fieldIndex))
        {
            var matchingRecordIds = new HashSet<string>(fieldIndex.SearchTerm(fieldValue));
            foreach (var recordId in fieldIndex.SearchWildcard(GetWildcardPattern(fieldValue)))
            {
                matchingRecordIds.Add(recordId);
            }
            return matchingRecordIds.Select(id => records[id]);
        }
        return Enumerable.Empty<Record>();
    }

    private static string GetWildcardPattern(string wildcard)
    {
        return wildcard.Replace("*", "");
    }
}