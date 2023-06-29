namespace InvertedIndex;

public class Record
{
    public string Id { get; set; }
    public Dictionary<string, string> Fields { get; set; }
}

public class InvertedIndex
{
    private readonly Dictionary<string, HashSet<string>> index = new Dictionary<string, HashSet<string>>();

    public void AddTerm(string term, string recordId)
    {
        if (!index.TryGetValue(term, out var recordIds))
        {
            recordIds = new HashSet<string>();
            index[term] = recordIds;
        }
        recordIds.Add(recordId);
    }

    public IEnumerable<string> GetMatchingRecordIds(IEnumerable<string> terms)
    {
        var matchingRecordIds = new HashSet<string>();
        foreach (var term in terms)
        {
            if (index.TryGetValue(term, out var recordIds))
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
    private readonly Dictionary<string, InvertedIndex> fieldIndexes = new Dictionary<string, InvertedIndex>();

    public void AddRecord(Record record)
    {
        records[record.Id] = record;

        foreach (var field in record.Fields)
        {
            if (!fieldIndexes.TryGetValue(field.Key, out var index))
            {
                index = new InvertedIndex();
                fieldIndexes[field.Key] = index;
            }

            var terms = field.Value.Split(' ');
            foreach (var term in terms)
            {
                index.AddTerm(term, record.Id);
            }
        }
    }

    public IEnumerable<Record> Search(string query)
    {
        var queryTerms = query.Split(' ');
        var matchingRecordIds = fieldIndexes.Values
            .Select(index => index.GetMatchingRecordIds(queryTerms))
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
    private static void InvertedIndex()
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