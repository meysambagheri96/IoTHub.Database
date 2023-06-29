/*
High Performance Scalable In-Memory Data Structure 
- Lucene Algorithm 
- Analyzer
- Indexer
*/

using System;
using System.Collections;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.ComponentModel;
using System.Linq;
using System.Threading.Tasks;
using Lucene.Net.Analysis;
using Lucene.Net.Analysis.Standard;
using Lucene.Net.Documents;
using Lucene.Net.Index;
using Lucene.Net.Search;
using Lucene.Net.Store;

public class InMemoryAnalyzerIndexerDatabase
{
    private readonly ConcurrentDictionary<string, HashSet<string>> data;
    private readonly ConcurrentDictionary<string, Dictionary<string, HashSet<string>>> index;
    private readonly ConcurrentDictionary<string, BloomFilter> bloomFilters;
    private readonly ParallelOptions parallelOptions;
    private readonly Analyzer analyzer;
    private readonly IndexWriter indexWriter;
    private readonly IndexSearcher indexSearcher;

    public InMemoryAnalyzerIndexerDatabase()
    {
        data = new ConcurrentDictionary<string, HashSet<string>>();
        index = new ConcurrentDictionary<string, Dictionary<string, HashSet<string>>>();
        bloomFilters = new ConcurrentDictionary<string, BloomFilter>();
        parallelOptions = new ParallelOptions { MaxDegreeOfParallelism = Environment.ProcessorCount };
        analyzer = new StandardAnalyzer(Lucene.Net.Util.Version.LUCENE_30);
        indexWriter = new IndexWriter(new RAMDirectory(), analyzer, IndexWriter.MaxFieldLength.UNLIMITED);
        indexSearcher = new IndexSearcher(indexWriter.GetReader());
    }

    public void AddRecord(string recordId, Dictionary<string, string> fields)
    {
        Parallel.ForEach(fields, parallelOptions, field =>
        {
            string fieldName = field.Key;
            string fieldValue = field.Value;

            data.AddOrUpdate(fieldName, new HashSet<string> { fieldValue }, (_, existingValue) =>
            {
                existingValue.Add(fieldValue);
                return existingValue;
            });

            index.AddOrUpdate(fieldName, new Dictionary<string, HashSet<string>> { { fieldValue, new HashSet<string> { recordId } } }, (_, existingValue) =>
            {
                existingValue.AddOrUpdate(fieldValue, new HashSet<string> { recordId }, (_, existingRecords) =>
                {
                    existingRecords.Add(recordId);
                    return existingRecords;
                });
                return existingValue;
            });

            bloomFilters.AddOrUpdate(fieldName, new BloomFilter(), (_, existingFilter) =>
            {
                existingFilter.Add(fieldValue);
                return existingFilter;
            });
        });

        var document = new Document();

        foreach (var field in fields)
        {
            string fieldName = field.Key;
            string fieldValue = field.Value;

            document.Add(new Field(fieldName, fieldValue, Field.Store.YES, Field.Index.ANALYZED));
        }

        indexWriter.AddDocument(document);
    }

    public List<string> Search(string fieldName, string searchTerm)
    {
        if (!index.ContainsKey(fieldName))
        {
            return new List<string>();
        }

        var bloomFilter = bloomFilters[fieldName];

        if (!bloomFilter.Contains(searchTerm))
        {
            return new List<string>();
        }

        var query = GetQuery(fieldName, searchTerm);

        var hits = indexSearcher.Search(query, int.MaxValue);

        List<string> result = new List<string>();

        foreach (var hit in hits.ScoreDocs)
        {
            var document = indexSearcher.Doc(hit.Doc);
            result.Add(document.ToString());
        }

        return result;
    }

    public List<string> WildcardSearch(string fieldName, string searchTerm)
    {
        if (!data.ContainsKey(fieldName))
        {
            return new List<string>();
        }

        if (!searchTerm.Contains("*"))
        {
            return Search(fieldName, searchTerm);
        }

        var prefix = searchTerm.Split('*')[0];
        var result = data[fieldName].Where(value => value.StartsWith(prefix)).ToList();
        return result;
    }

    private Query GetQuery(string fieldName, string searchTerm)
    {
        var parser = new QueryParser(Lucene.Net.Util.Version.LUCENE_30, fieldName, analyzer);
        parser.AllowLeadingWildcard = true;

        if (searchTerm.Contains("*"))
        {
            searchTerm = searchTerm.Replace('*', '?');
        }

        var query = parser.Parse(searchTerm);

        return query;
    }

    public void Compress()
    {
        Parallel.ForEach(data, parallelOptions, field =>
        {
            data[field.Key] = new HashSet<string>(field.Value.OrderBy(value => value));
        });

        Parallel.ForEach(index, parallelOptions, field =>
        {
            index[field.Key] = new Dictionary<string, HashSet<string>>(field.Value.ToDictionary(indexField => indexField.Key, indexField => new HashSet<string>(indexField.Value.OrderBy(recordId => recordId))));
        });
    }
}