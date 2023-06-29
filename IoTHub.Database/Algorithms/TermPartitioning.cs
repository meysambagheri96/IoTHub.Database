namespace TermPartitioning;

public class Record
{
    public string Id { get; set; }
    public Dictionary<string, string> Fields { get; set; }
}

public class TermPartitioning
{
    private readonly Dictionary<string, HashSet<string>>[] partitions;

    public TermPartitioning(int numPartitions)
    {
        partitions = new Dictionary<string, HashSet<string>>[numPartitions];
        for (var i = 0; i < numPartitions; i++)
        {
            partitions[i] = new Dictionary<string, HashSet<string>>();
        }
    }

    private int GetPartitionIndex(string term)
    {
        return Math.Abs(term.GetHashCode()) % partitions.Length;
    }

    public void AddTerm(string term, string recordId)
    {
        var partitionIndex = GetPartitionIndex(term);
        if (!partitions[partitionIndex].TryGetValue(term, out var recordIds))
        {
            recordIds = new HashSet<string>();
            partitions[partitionIndex][term] = recordIds;
        }
        recordIds.Add(recordId);
    }

    public IEnumerable<string> GetMatchingRecordIds(IEnumerable<string> terms)
    {
        var matchingRecordIds = new HashSet<string>();
        foreach (var term in terms)
        {
            var partitionIndex = GetPartitionIndex(term);
            if (partitions[partitionIndex].TryGetValue(term, out var recordIds))
            {
                if (matchingRecordIds.Count == 0)
                {
                    matchingRecordIds = recordIds;
                }
                else
                {
                    matchingRecordIds.IntersectWith(recordIds);
                }
            }
            else
            {
                matchingRecordIds.Clear();
                break;
            }
        }
        return matchingRecordIds;
    }
}

public class Database
{
    private readonly Dictionary<string, Record> records = new Dictionary<string, Record>();
    private readonly Dictionary<string, TermPartitioning> fieldPartitions = new Dictionary<string, TermPartitioning>();

    public void AddRecord(Record record)
    {
        records[record.Id] = record;

        foreach (var field in record.Fields)
        {
            if (!fieldPartitions.TryGetValue(field.Key, out var partitioning))
            {
                partitioning = new TermPartitioning(1000);
                fieldPartitions[field.Key] = partitioning;
            }

            var terms = field.Value.Split(' ');
            foreach (var term in terms)
            {
                partitioning.AddTerm(term, record.Id);
            }
        }
    }

    public IEnumerable<Record> Search(string query)
    {
        var queryTerms = query.Split(' ');
        var matchingRecordIds = fieldPartitions.Values
            .Select(partitioning => partitioning.GetMatchingRecordIds(queryTerms))
            .Aggregate((a, b) => a.Intersect(b))
            .Distinct();

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

public class Program
{
    private static void TermPartitioning()
    {
        // Create a database
        var database = new Database();

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