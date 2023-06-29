namespace IoTHub.Database;

public static class Program
{
    public static void Main()
    {
        var db = new InMemoryDictionaryDatabase.InMemoryDictionaryDatabase();

        for (int i = 0; i < 150_000_000; i++)
        {
            var record = new Dictionary<string, object>()
            {
                { "Device", "Apple iPhone 14 Pro Max" },
                {
                    "Bands",
                    "SA/NSA/Sub6 - A2894, A2896 261 SA/NSA/Sub6/mmWave - A2651 SA/NSA/Sub6 - A2893 SA/NSA/Sub6 - A2895"
                },
                {
                    "Versions",
                    "A2894 (International); A2651 (USA); A2893 (Canada, Japan); A2896 (China, Hong Kong); A2895 (Russia)"
                },
                { "OS", "iOS 16, upgradable to iOS 16.5, planned upgrade to iOS 17" },
                { "Chipset", "Apple A16 Bionic (4 nm)" },
                { "CPU", "Hexa-core (2x3.46 GHz Everest + 4x2.02 GHz Sawtooth)" },
                { "GPU", "Apple GPU (5-core graphics)" },
                { "WLAN", "Wi-Fi 802.11 a/b/g/n/ac/6, dual-band, hotspot" },
                { "Sensors", "Face ID, accelerometer, gyro, proximity, compass, baromete" }
            };

            db.AddRecord(record);
        }

        var result =db.TermSearch("Sensors", "Face");

        Console.ReadLine();
    }
}