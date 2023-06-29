namespace IoTHub.Database.Contracts;

public interface IInMemoryDatabase
{
    public void AddRecord(Dictionary<string, object> record);
    public IEnumerable<Dictionary<string, object>> TermSearch(string fieldName, object value);
    public IEnumerable<Dictionary<string, object>> WildcardSearch(string fieldName, object value);
}