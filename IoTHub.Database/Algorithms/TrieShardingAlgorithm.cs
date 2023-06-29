namespace TrieShardingAlgorithm;

public class Record
{
    public string Id { get; set; }
    public Dictionary<string, string> Fields { get; set; }
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

public class Shard
{
    private readonly Dictionary<string, Record> records = new Dictionary<string, Record>();
    private readonly Dictionary<string, Trie> fieldTries = new Dictionary<string, Trie>();

    public void AddRecord(Record record)
    {
        records[record.Id] = record;

        foreach (var field in record.Fields)
        {
            if (!fieldTries.TryGetValue(field.Key, out var trie))
            {
                trie = new Trie();
                fieldTries[field.Key] = trie;
            }

            var words = field.Value.Split(' ');
            foreach (var word in words)
            {
                trie.Add(word, record.Id);
            }
        }
    }

    public IEnumerable<Record> Search(string query)
    {
        var queryTerms = query.Split(' ');
        var matchingRecordIds = new HashSet<string>();
        foreach (var fieldTrie in fieldTries.Values)
        {
            foreach (var queryTerm in queryTerms)
            {
                foreach (var recordId in fieldTrie.Search(queryTerm))
                {
                    matchingRecordIds.Add(recordId);
                }
            }
        }

        foreach (var recordId in matchingRecordIds)
        {
            if (records.TryGetValue(recordId, out var record))
            {
                var allFieldsMatch = true;
                foreach (var field in record.Fields)
                {
                    if (!queryTerms.All(term => field.Value.Contains(term)))
                    {
                        allFieldsMatch = false;
                        break;
                    }
                }
                if (allFieldsMatch)
                {
                    yield return record;
                }
            }
        }
    }
}

public class Database
{
    private readonly Shard[] shards;

    public Database(int numShards)
    {
        shards = new Shard[numShards];
        for (var i = 0; i < numShards; i++)
        {
            shards[i] = new Shard();
        }
    }

    public void AddRecord(Record record)
    {
        var shardIndex = GetShardIndex(record.Id);
        shards[shardIndex].AddRecord(record);
    }

    public IEnumerable<Record> Search(string query)
    {
        if (query.Contains('*'))
        {
            // Wildcard search
            var matchingRecords = new HashSet<Record>();
            Parallel.ForEach(shards, shard =>
            {
                foreach (var record in shard.Search(query))
                {
                    lock (matchingRecords)
                    {
                        matchingRecords.Add(record);
                    }
                }
            });
            return matchingRecords;
        }
        else
        {
            // Term search
            var matchingRecords = new List<Record>();
            Parallel.ForEach(shards, shard =>
            {
                foreach (var record in shard.Search(query))
                {
                    lock (matchingRecords)
                    {
                        matchingRecords.Add(record);
                    }
                }
            });
            return matchingRecords;
        }
    }

    private int GetShardIndex(string recordId)
    {
        // Use the first two characters of the record ID to determine the shard index
        return int.Parse(recordId.Substring(0, 2)) % shards.Length;
    }
}

public class Program
{
    private static void TrieShardingAlgorithm()
    {
        // Create a database with 16 shards
        var database = new Database(16);

        // Add a large set of records with dynamic fields to the database
        var numRecords = 1000000000;
        for (var i = 0; i < numRecords; i++)
        {
            var record = new Record
            {
                Id = i.ToString(),
                Fields = new Dictionary<string, string>
                {
                    ["Title"] = "Record " + i.ToString(),
                    ["Description"] = "This is record " + i.ToString(),
                    ["Keywords"] = "keyword" + i.ToString() + " foo bar"
                }
            };
            database.AddRecord(record);
        }

        // Perform some search queries
        var queries = new[] { "Record", "100000", "foo", "keyword*", "foo bar" };
        foreach (var query in queries)
        {
            var matchingRecords = database.Search(query);
            Console.WriteLine("Matching records for query '{0}': {1}", query, matchingRecords.Count());
        }
    }
}