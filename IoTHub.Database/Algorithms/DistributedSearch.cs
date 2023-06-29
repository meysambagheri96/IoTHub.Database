namespace DistributedSearch;

public class Record
{
    public string Id { get; set; }
    public Dictionary<string, string> Fields { get; set; }
}

public class DistributedSearch
{
    private readonly Dictionary<string, HashSet<string>>[] nodeIndexes;

    public DistributedSearch(int numNodes)
    {
        nodeIndexes = new Dictionary<string, HashSet<string>>[numNodes];
        for (var i = 0; i < numNodes; i++)
        {
            nodeIndexes[i] = new Dictionary<string, HashSet<string>>();
        }
    }

    private int GetNodeIndex(string term)
    {
        return Math.Abs(term.GetHashCode()) % nodeIndexes.Length;
    }

    public void AddTerm(string term, string recordId)
    {
        var nodeIndex = GetNodeIndex(term);
        if (!nodeIndexes[nodeIndex].TryGetValue(term, out var recordIds))
        {
            recordIds = new HashSet<string>();
            nodeIndexes[nodeIndex][term] = recordIds;
        }
        recordIds.Add(recordId);
    }

    public IEnumerable<string> GetMatchingRecordIds(IEnumerable<string> terms)
    {
        var matchingRecordIds = new HashSet<string>();
        foreach (var term in terms)
        {
            var nodeIndex = GetNodeIndex(term);
            if (nodeIndexes[nodeIndex].TryGetValue(term, out var recordIds))
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
    private readonly Dictionary<string, DistributedSearch> fieldSearches = new Dictionary<string, DistributedSearch>();

    public void AddRecord(Record record)
    {
        records[record.Id] = record;

        foreach (var field in record.Fields)
        {
            if (!fieldSearches.TryGetValue(field.Key, out var search))
            {
                search = new DistributedSearch(10);
                fieldSearches[field.Key] = search;
            }

            var terms = field.Value.Split(' ');
            foreach (var term in terms)
            {
                search.AddTerm(term, record.Id);
            }
        }
    }

    public IEnumerable<Record> Search(string query)
    {
        var queryTerms = query.Split(' ');
        var matchingRecordIds = fieldSearches.Values
            .Select(search => search.GetMatchingRecordIds(queryTerms))
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
    private static void DistributedSearch()
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