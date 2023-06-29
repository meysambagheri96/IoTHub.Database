using IoTHub.Database.InMemoryTrieAlgorithmDataBase;

namespace IoTHub.Database;

public static class Program
{
    public static void Main()
    {
        var db = new InMemoryTrieAlgorithmDataBase.InMemoryTrieAlgorithmDataBase();

        for (int i = 0; i < 15_000_000; i++)
        {
            var record = new Dictionary<string, string>()
            {
                { "Device", $"Apple{i} iPhone 14 Pro Max" },
                {
                    "Bands",
                    $"{i}SA/NSA/Sub6 - A2894, A2896 261 SA/NSA/Sub6/mmWave - A2651 SA/NSA/Sub6 - A2893 SA/NSA/Sub6 - A2895"
                },
                {
                    "Versions",
                    $"{i}A2894 (International); A2651 (USA); A2893 (Canada, Japan); A2896 (China, Hong Kong); A2895 (Russia)"
                },
                { "OS", $"{i}iOS 16, upgradable to iOS 16.5, planned upgrade to iOS 17" },
                { "Chipset", $"{i}Apple A16 Bionic (4 nm)" },
                { "CPU", $"{i}Hexa-core (2x3.46 GHz Everest + 4x2.02 GHz Sawtooth)" },
                { "GPU", "Apple GPU (5-core graphics)" },
                { "WLAN", $"{i}Wi-Fi 802.11 a/b/g/n/ac/6, dual-band, hotspot" },
                { "Sensors", $"{i}Face ID, accelerometer, gyro, proximity, compass, baromete" }
            };

            db.InsertRecord(new Record(record));
        }

        var result = db.SearchTerm("Sensors20");

        Console.ReadLine();
    }
}