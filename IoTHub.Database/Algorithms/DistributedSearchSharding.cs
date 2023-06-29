namespace DistributedSearchSharding;

public class Record
{
    public string Id { get; set; }
    public Dictionary<string, string> Fields { get; set; }
}

public class Shard
{
    private readonly Dictionary<string, Record> records = new Dictionary<string, Record>();

    public void AddRecord(Record record)
    {
        records[record.Id] = record;
    }

    public IEnumerable<Record> Search(string query)
    {
        return records.Values.Where(record => record.Fields.Values.Any(fieldValue => fieldValue.Contains(query)));
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

    private int GetShardIndex(string recordId)
    {
        // Use the first two characters of the record ID to determine the shard index
        return int.Parse(recordId.Substring(0, 2)) % shards.Length;
    }
}

public class Program
{
    private static void DistributedSearchSharding()
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
                    ["Description"] = "This is record " + i.ToString()
                }
            };
            database.AddRecord(record);
        }

        // Perform some search queries
        var queries = new[] { "Record", "100000", "foo" };
        foreach (var query in queries)
        {
            var matchingRecords = database.Search(query);
            Console.WriteLine("Matching records for query '{0}': {1}", query, matchingRecords.Count());
        }
    }
}