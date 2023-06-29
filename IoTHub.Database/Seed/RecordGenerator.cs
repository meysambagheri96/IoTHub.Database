namespace IoTHub.Database.Seed;

public static class RecordGenerator
{
    private static readonly Random Random = new Random();

    public static Dictionary<string, object> GenerateRecord(int numFields)
    {
        var record = new Dictionary<string, object>();
        for (int i = 0; i < numFields; i++)
        {
            string fieldName = $"field{i}";
            object fieldValue = GenerateRandomValue();
            record[fieldName] = fieldValue;
        }
        return record;
    }

    private static object GenerateRandomValue()
    {
        int randomInt = Random.Next();
        double randomDouble = Random.NextDouble();
        bool randomBool = Random.Next(2) == 0 ? false : true;
        string randomString = Guid.NewGuid().ToString();
        return Random.Next(4) switch
        {
            0 => randomInt,
            1 => randomDouble,
            2 => randomBool,
            3 => randomString,
            _ => null,
        };
    }
}