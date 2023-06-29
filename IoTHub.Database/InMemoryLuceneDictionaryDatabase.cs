using Lucene.Net.Analysis;
using Lucene.Net.Documents;
using Lucene.Net.Index;
using Lucene.Net.Search;
using Lucene.Net.Store;
using Lucene.Net.Util;
using Directory = Lucene.Net.Store.Directory;

public class InMemoryLuceneDictionaryDatabase
{
    private readonly List<Machine> _machines = new List<Machine>();
    private readonly Dictionary<string, List<Machine>> _shardLookup = new Dictionary<string, List<Machine>>();

    public InMemoryLuceneDictionaryDatabase(int numShards, int numMachinesPerShard)
    {
        // Create machines and shards
        for (int i = 0; i < numShards; i++)
        {
            List<Machine> shardMachines = new List<Machine>();
            for (int j = 0; j < numMachinesPerShard; j++)
            {
                Machine machine = new Machine();
                shardMachines.Add(machine);
                _machines.Add(machine);
            }
            _shardLookup.Add(i.ToString(), shardMachines);
        }
    }

    public void AddRecord(Dictionary<string, object> record)
    {
        // Hash record content to determine shard ID
        string content = string.Join('|', record.Values);
        int shardId = Math.Abs(content.GetHashCode()) % _shardLookup.Count;

        // Add record to appropriate machines in shard
        List<Machine> shardMachines = _shardLookup[shardId.ToString()];
        foreach (Machine machine in shardMachines)
        {
            machine.AddRecord(record);
        }
    }

    public IEnumerable<Dictionary<string, object>> TermSearch(string fieldName, string term)
    {
        // Search all machines in all shards
        foreach (Machine machine in _machines)
        {
            foreach (Dictionary<string, object> record in machine.TermSearch(fieldName, term))
            {
                yield return record;
            }
        }
    }

    public IEnumerable<Dictionary<string, object>> WildcardSearch(string fieldName, string prefix)
    {
        // Search all machines in all shards
        foreach (Machine machine in _machines)
        {
            foreach (Dictionary<string, object> record in machine.WildcardSearch(fieldName, prefix))
            {
                yield return record;
            }
        }
    }
}

public class Machine
{
    private Dictionary<string, IndexWriter> _indexWriters = new Dictionary<string, IndexWriter>();

    public void AddRecord(Dictionary<string, object> record)
    {
        // Index each field in the record
        foreach (KeyValuePair<string, object> field in record)
        {
            string fieldName = field.Key;
            string fieldValue = field.Value?.ToString() ?? "";

            // Get or create index writer for field
            if (!_indexWriters.ContainsKey(fieldName))
            {
                Directory indexDirectory = new RAMDirectory();
                //todo
                IndexWriterConfig config = new IndexWriterConfig(LuceneVersion.LUCENE_48, null /*new KeywordAnalyzer()*/);
                IndexWriter indexWriter = new IndexWriter(indexDirectory, config);
                _indexWriters.Add(fieldName, indexWriter);
            }
            IndexWriter writer = _indexWriters[fieldName];

            // Add document to index
            Document document = new Document();
            document.Add(new StringField("id", fieldName, Field.Store.YES));
            document.Add(new TextField("content", fieldValue, Field.Store.NO));
            writer.AddDocument(document);
        }
    }

    public IEnumerable<Dictionary<string, object>> TermSearch(string fieldName, string term)
    {
        // Search index for field
        if (_indexWriters.ContainsKey(fieldName))
        {
            DirectoryReader reader = DirectoryReader.Open(_indexWriters[fieldName], false);
            IndexSearcher searcher = new IndexSearcher(reader);
            Query query = new TermQuery(new Term("content", term));
            TopDocs topDocs = searcher.Search(query, int.MaxValue);
            foreach (ScoreDoc scoreDoc in topDocs.ScoreDocs)
            {
                Document document = searcher.Doc(scoreDoc.Doc);
                yield return new Dictionary<string, object> {
                    { document.Get("id"), document.Get("content") }
                };
            }
            reader.Dispose();
        }
    }

    public IEnumerable<Dictionary<string, object>> WildcardSearch(string fieldName, string prefix)
    {
        // Search index forfield
        if (_indexWriters.ContainsKey(fieldName))
        {
            DirectoryReader reader = DirectoryReader.Open(_indexWriters[fieldName], false);
            IndexSearcher searcher = new IndexSearcher(reader);
            Query query = new PrefixQuery(new Term("content", prefix));
            TopDocs topDocs = searcher.Search(query, int.MaxValue);
            foreach (ScoreDoc scoreDoc in topDocs.ScoreDocs)
            {
                Document document = searcher.Doc(scoreDoc.Doc);
                yield return new Dictionary<string, object> {
                    { document.Get("id"), document.Get("content") }
                };
            }
            reader.Dispose();
        }
    }
}