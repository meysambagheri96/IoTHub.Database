namespace TrieShardingTermWildcardAlgorithm;

public class Record
{
    public string Id { get; set; }
    public Dictionary<string, string> Fields { get; set; }
}

public class FieldIndex
{
    private readonly Dictionary<string, HashSet<string>> termIndex = new Dictionary<string, HashSet<string>>();
    private readonly Dictionary<string, Trie> trieIndex = new Dictionary<string, Trie>();

    public void AddTerm(string term, string recordId)
    {
        if (!termIndex.TryGetValue(term, out var recordIds))
        {
            recordIds = new HashSet<string>();
            termIndex[term] = recordIds;
        }
        recordIds.Add(recordId);
    }

    public void AddWildcard(string wildcard, string recordId)
    {
        if (!trieIndex.TryGetValue(wildcard, out var trie))
        {
            trie = new Trie();
            trieIndex[wildcard] = trie;
        }
        trie.Add(wildcard, recordId);
    }

    public IEnumerable<string> SearchTerm(string term)
    {
        if (termIndex.TryGetValue(term, out var recordIds))
        {
            return recordIds;
        }
        return Enumerable.Empty<string>();
    }

    public IEnumerable<string> SearchWildcard(string wildcard)
    {
        if (trieIndex.TryGetValue(wildcard, out var trie))
        {
            return trie.Search(wildcard);
        }
        return Enumerable.Empty<string>();
    }
}

public class Trie
{
    private readonly TrieNode root = new TrieNode();

    public void Add(string word, string recordId)
    {
        var current = root;
        foreach (var c in word)
        {
            if (!current.Children.TryGetValue(c, out var node))
            {
                node = new TrieNode();
                current.Children[c] = node;
            }
            current = node;
        }
        current.RecordIds.Add(recordId);
    }

    public IEnumerable<string> Search(string query)
    {
        var current = root;
        foreach (var c in query)
        {
            if (c == '*')
            {
                return current.GetAllRecordIds();
            }
            if (!current.Children.TryGetValue(c, out var node))
            {
                return new List<string>();
            }
            current = node;
        }
        return current.GetAllRecordIds();
    }

    private class TrieNode
    {
        public readonly Dictionary<char, TrieNode> Children = new Dictionary<char, TrieNode>();
        public readonly HashSet<string> RecordIds = new HashSet<string>();

        public IEnumerable<string> GetAllRecordIds()
        {
            var allRecordIds = new HashSet<string>(RecordIds);
            foreach (var child in Children.Values)
            {
                foreach (var recordId in child.GetAllRecordIds())
                {
                    allRecordIds.Add(recordId);
                }
            }
            return allRecordIds;
        }
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

            fieldIndex.AddWildcard(field.Value, record.Id);
        }
    }

    public IEnumerable<Record> Search(string fieldName, string fieldValue)
    {
        if (fieldIndexes.TryGetValue(fieldName, out var fieldIndex))
        {
            var matchingRecordIds = new HashSet<string>(fieldIndex.SearchTerm(fieldValue));
            foreach (var wildcard in fieldValue.Split(' '))
            {
                foreach (var recordId in fieldIndex.SearchWildcard(wildcard))
                {
                    matchingRecordIds.Add(recordId);
                }
            }
            return matchingRecordIds.Select(id => records[id]);
        }
        return Enumerable.Empty<Record>();
    }
}