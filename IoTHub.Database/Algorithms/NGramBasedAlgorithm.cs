namespace NGramBasedAlgorithm;

public class Record
{
    public string Id { get; set; }
    public Dictionary<string, string> Fields { get; set; }
}

public class FieldIndex
{
    private readonly Dictionary<string, HashSet<string>> termIndex = new Dictionary<string, HashSet<string>>();
    private readonly Dictionary<string, HashSet<string>> ngramIndex = new Dictionary<string, HashSet<string>>();

    public void AddTerm(string term, string recordId)
    {
        if (!termIndex.TryGetValue(term, out var recordIds))
        {
            recordIds = new HashSet<string>();
            termIndex[term] = recordIds;
        }
        recordIds.Add(recordId);

        foreach (var ngram in GetNGrams(term))
        {
            if (!ngramIndex.TryGetValue(ngram, out var ngramRecordIds))
            {
                ngramRecordIds = new HashSet<string>();
                ngramIndex[ngram] = ngramRecordIds;
            }
            ngramRecordIds.Add(recordId);
        }
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
        var wildcardNgrams = GetNGrams(wildcard);
        var matchingRecordIds = new HashSet<string>();
        foreach (var ngram in wildcardNgrams)
        {
            if (ngramIndex.TryGetValue(ngram, out var ngramRecordIds))
            {
                foreach (var recordId in ngramRecordIds)
                {
                    matchingRecordIds.Add(recordId);
                }
            }
        }
        return matchingRecordIds;
    }

    private IEnumerable<string> GetNGrams(string term)
    {
        const int n = 3; // Use trigrams
        term = $"#{term}#"; // Add boundary markers
        for (int i = 0; i <= term.Length - n; i++)
        {
            yield return term.Substring(i, n);
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
        }
    }

    private static string GetWildcardPattern(string wildcard)
    {
        return wildcard.Replace("*", ".*");
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
}