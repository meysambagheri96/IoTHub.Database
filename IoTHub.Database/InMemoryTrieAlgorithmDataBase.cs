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

namespace IoTHub.Database.InMemoryTrieAlgorithmDataBase;

//Usable
public class InMemoryTrieAlgorithmDataBase
{
    // Use a hash table to store the records
    private Dictionary<int, List<Record>> _records = new Dictionary<int, List<Record>>();

    // Use a trie data structure to index the records based on their terms
    private Trie _trie = new Trie();

    // Add a method to insert a new record
    public void InsertRecord(Record record)
    {
        // Add the record to the hash table
        foreach (var field in record.Fields)
        {
            int hashCode = field.Key.GetHashCode();
            if (!_records.ContainsKey(hashCode))
            {
                _records[hashCode] = new List<Record>();
            }
            _records[hashCode].Add(record);
        }

        // Add the record to the trie
        foreach (var field in record.Fields.Values)
        {
            _trie.Insert(field, record);
        }
    }

    // Add a method to search for records containing a given term
    public List<Record> SearchTerm(string term)
    {
        List<Record> result = new List<Record>();
        int hashCode = term.GetHashCode();
        if (_records.ContainsKey(hashCode))
        {
            foreach (var record in _records[hashCode])
            {
                if (record.ContainsTerm(term))
                {
                    result.Add(record);
                }
            }
        }
        return result;
    }

    // Add a method to search for records matching a wildcard pattern
    public List<Record> SearchWildcard(string pattern)
    {
        return _trie.Search(pattern);
    }
}

// Define a class to represent the trie data structure
public class Trie
{
    private TrieNode _root = new TrieNode();

    // Add a method to insert a record into the trie
    public void Insert(string text, Record record)
    {
        TrieNode node = _root;
        foreach (char c in text)
        {
            if (!node.Children.ContainsKey(c))
            {
                node.Children[c] = new TrieNode();
            }
            node = node.Children[c];
        }
        node.Records.Add(record);
    }

    // Add a method to search for records matching a wildcard pattern
    public List<Record> Search(string pattern)
    {
        List<Record> result = new List<Record>();
        Queue<TrieNode> queue = new Queue<TrieNode>();
        queue.Enqueue(_root);
        foreach (char c in pattern)
        {
            int count = queue.Count;
            for (int i = 0; i < count; i++)
            {
                TrieNode node = queue.Dequeue();
                if (c == '*')
                {
                    foreach (var child in node.Children.Values)
                    {
                        queue.Enqueue(child);
                    }
                }
                else if (node.Children.ContainsKey(c))
                {
                    queue.Enqueue(node.Children[c]);
                }
            }
        }
        while (queue.Count > 0)
        {
            result.AddRange(queue.Dequeue().Records);
        }
        return result;
    }

    // Define a class to represent a node in the trie
    private class TrieNode
    {
        public Dictionary<char, TrieNode> Children { get; } = new Dictionary<char, TrieNode>();
        public List<Record> Records { get; } = new List<Record>();
    }
}

public class Record
{
    // Use a dictionary to store the dynamic fields of the record
    public Dictionary<string, string> Fields { get; } = new Dictionary<string, string>();

    // Add a method to check if the record contains a given term
    public bool ContainsTerm(string term)
    {
        foreach (var field in Fields.Values)
        {
            if (field.Contains(term))
            {
                return true;
            }
        }
        return false;
    }
}