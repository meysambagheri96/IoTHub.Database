using System.Security.Cryptography;

namespace BloomFilter;

public class Record
{
    public string Id { get; set; }
    public Dictionary<string, string> Fields { get; set; }
}

public class BloomFilter
{
    private readonly byte[] bits;
    private readonly int numHashFunctions;

    public BloomFilter(int numBits, int numHashFunctions)
    {
        bits = new byte[numBits / 8];
        this.numHashFunctions = numHashFunctions;
    }

    private int[] GetHashValues(string value)
    {
        using var hashAlgorithm = SHA256.Create();
        var hashBytes = hashAlgorithm.ComputeHash(System.Text.Encoding.UTF8.GetBytes(value));
        var hashValues = new List<int>();
        for (var i = 0; i < numHashFunctions; i++)
        {
            var hashValue = BitConverter.ToInt32(hashBytes, i * sizeof(int));
            hashValues.Add(Math.Abs(hashValue) % bits.Length);
        }
        return hashValues.ToArray();
    }

    public void AddValue(string value)
    {
        foreach (var hashValue in GetHashValues(value))
        {
            bits[hashValue / 8] |= (byte)(1 << (hashValue % 8));
        }
    }

    public bool ContainsValue(string value)
    {
        foreach (var hashValue in GetHashValues(value))
        {
            if ((bits[hashValue / 8] & (1 << (hashValue % 8))) == 0)
            {
                return false;
            }
        }
        return true;
    }
}

public class Database
{
    private readonly Dictionary<string, Record> records = new Dictionary<string, Record>();
    private readonly Dictionary<string, HashSet<string>> termIndex = new Dictionary<string, HashSet<string>>();
    private readonly Dictionary<string, BloomFilter> fieldFilters = new Dictionary<string, BloomFilter>();

    public void AddRecord(Record record)
    {
        records[record.Id] = record;

        foreach (var field in record.Fields)
        {
            if (!fieldFilters.TryGetValue(field.Key, out var filter))
            {
                filter = new BloomFilter(100000000, 5);
                fieldFilters[field.Key] = filter;
            }
            filter.AddValue(field.Value);

            var terms = field.Value.Split(' ');
            foreach (var term in terms)
            {
                if (!termIndex.TryGetValue(term, out var recordIds))
                {
                    recordIds = new HashSet<string>();
                    termIndex[term] = recordIds;
                }
                recordIds.Add(record.Id);
            }
        }
    }

    public IEnumerable<Record> Search(string query)
    {
        var queryTerms = query.Split(' ');
        var matchingRecordIds = new HashSet<string>();
        foreach (var term in queryTerms)
        {
            if (termIndex.TryGetValue(term, out var recordIds))
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

        foreach (var recordId in matchingRecordIds)
        {
            if (records.TryGetValue(recordId, out var record))
            {
                var allFieldsMatch = true;
                foreach (var field in record.Fields)
                {
                    if (fieldFilters.TryGetValue(field.Key, out var filter))
                    {
                        if (!filter.ContainsValue(field.Value))
                        {
                            allFieldsMatch = false;
                            break;
                        }
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
    private static void BloomFilter()
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