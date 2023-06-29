namespace RegularExpressionAlgorithm;

using System.Collections.Generic;
using System.Linq;
using System.Text.RegularExpressions;

public class Record
{
    public string Id { get; set; }
    public Dictionary<string, string> Fields { get; set; }
}

public class FieldIndex
{
    private readonly Dictionary<string, HashSet<string>> termIndex = new Dictionary<string, HashSet<string>>();
    private readonly Regex wildcardRegex;

    public FieldIndex(string wildcard)
    {
        wildcardRegex = new Regex(wildcard.Replace("*", ".*"), RegexOptions.Compiled | RegexOptions.IgnoreCase);
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

    public bool MatchWildcard(string value)
    {
        return wildcardRegex.IsMatch(value);
    }

    public IEnumerable<string> SearchTerm(string term)
    {
        if (termIndex.TryGetValue(term, out var recordIds))
        {
            return recordIds;
        }
        return Enumerable.Empty<string>();
    }

    public IEnumerable<string> SearchWildcard()
    {
        return termIndex.Values.SelectMany(x => x).Where(x => MatchWildcard(x));
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
                fieldIndex = new FieldIndex("*");
                fieldIndexes[field.Key] = fieldIndex;
            }

            var words = field.Value.Split(' ');
            foreach (var word in words)
            {
                fieldIndex.AddTerm(word, record.Id);
            }

            var wildcard = "*" + field.Value + "*";
            if (!fieldIndexes.ContainsKey(wildcard))
            {
                fieldIndex = new FieldIndex(wildcard);
                fieldIndexes[wildcard] = fieldIndex;
            }
            fieldIndex.AddTerm(field.Value, record.Id);
        }
    }

    public IEnumerable<Record> Search(string fieldName, string fieldValue)
    {
        if (fieldIndexes.TryGetValue(fieldName, out var fieldIndex))
        {
            var matchingRecordIds = new HashSet<string>(fieldIndex.SearchTerm(fieldValue));
            foreach (var recordId in fieldIndex.SearchWildcard())
            {
                matchingRecordIds.Add(recordId);
            }
            return matchingRecordIds.Select(id => records[id]);
        }
        return Enumerable.Empty<Record>();
    }
}