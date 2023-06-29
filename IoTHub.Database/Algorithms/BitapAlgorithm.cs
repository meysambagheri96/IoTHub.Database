namespace BitapAlgorithm;

public class Record
{
    public string Id { get; set; }
    public Dictionary<string, string> Fields { get; set; }
}

public class FieldIndex
{
    private readonly Dictionary<string, HashSet<string>> termIndex = new Dictionary<string, HashSet<string>>();
    private readonly int maxErrors;

    public FieldIndex(int maxErrors)
    {
        this.maxErrors = maxErrors;
    }

    public void AddTerm(string term, string recordId)
    {
        if (!termIndex.TryGetValue(term, out var recordIds))
        {
            recordIds = new HashSet<string>();
            termIndex[term] = recordIds;
        }
        recordIds.Add(recordId);
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
        var pattern = GetBitapPattern(wildcard);
        var matchingRecordIds = new HashSet<string>();
        foreach (var term in termIndex.Keys)
        {
            if (BitapSearch(term, pattern, maxErrors))
            {
                foreach (var recordId in termIndex[term])
                {
                    matchingRecordIds.Add(recordId);
                }
            }
        }
        return matchingRecordIds;
    }

    private static int[] GetBitapPattern(string pattern)
    {
        const int alphabetSize = 256;
        var mask = new int[alphabetSize];
        var patternLength = pattern.Length;

        for (int i = 0; i < alphabetSize; i++)
        {
            mask[i] = ~0;
        }

        for (int i = 0; i < patternLength; i++)
        {
            mask[pattern[i]] &= ~(1 << i);
        }

        return mask;
    }

    private static bool BitapSearch(string text, int[] pattern, int maxErrors)
    {
        var textLength = text.Length;
        var patternLength = pattern.Length;
        var errors = 0;
        var score = 0;

        for (int i = 0; i < textLength; i++)
        {
            var oldScore = score;
            var letter = text[i];
            score <<= 1;
            score |= 1;
            score &= pattern[letter];

            if (score != 0)
            {
                errors++;
                if (errors > maxErrors)
                {
                    return false;
                }

                score |= (oldScore << 1) | 1;
                score &= pattern[letter];
            }

            if ((score & (1 << (patternLength - 1))) != 0)
            {
                return true;
            }
        }

        return false;
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
                fieldIndex = new FieldIndex(2); // Allow up to 2 errors for wildcard search
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
            foreach (var recordId in fieldIndex.SearchWildcard(fieldValue))
            {
                matchingRecordIds.Add(recordId);
            }
            return matchingRecordIds.Select(id => records[id]);
        }
        return Enumerable.Empty<Record>();
    }
}