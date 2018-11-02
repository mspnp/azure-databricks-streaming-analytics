namespace Taxi
{
    using System;
    using System.Globalization;
    using Newtonsoft.Json;
    using Newtonsoft.Json.Serialization;

    [JsonObject(NamingStrategyType = typeof(CamelCaseNamingStrategy))]
    public class TaxiFare : TaxiData

    {
        public TaxiFare()
        {
        }

        [JsonProperty]
        public string PaymentType { get; set; }

        [JsonProperty]
        public float FareAmount { get; set; }

        [JsonProperty]
        public float Surcharge { get; set; }

        [JsonProperty("mtaTax")]
        public float MTATax { get; set; }

        [JsonProperty]
        public float TipAmount { get; set; }

        [JsonProperty]
        public float TollsAmount { get; set; }

        [JsonProperty]
        public float TotalAmount { get; set; }

        public static TaxiFare FromString(string line,string header)
        {
            if (string.IsNullOrWhiteSpace(line))
            {
                throw new ArgumentException($"{nameof(line)} cannot be null, empty, or only whitespace");
            }

            string[] tokens = line.Split(',');
            if (tokens.Length != 11)
            {
                throw new ArgumentException($"Invalid record: {line}");
            }

            var fare = new TaxiFare();
            fare.CsvString = line;
            fare.CsvHeader = header;
            try
            {
                fare.Medallion = long.Parse(tokens[0]);
                fare.HackLicense = long.Parse(tokens[1]);
                fare.VendorId = tokens[2];
                fare.PickupTime = DateTimeOffset.ParseExact(
                    tokens[3], "yyyy-MM-dd HH:mm:ss",
                    CultureInfo.InvariantCulture,
                    DateTimeStyles.AssumeUniversal);
                fare.PaymentType = tokens[4];
                fare.FareAmount = float.TryParse(tokens[5], out float result) ? result : 0.0f;
                fare.Surcharge = float.TryParse(tokens[6], out result) ? result : 0.0f;
                fare.MTATax = float.TryParse(tokens[7], out result) ? result : 0.0f;
                fare.TipAmount = float.TryParse(tokens[8], out result) ? result : 0.0f;
                fare.TollsAmount = float.TryParse(tokens[9], out result) ? result : 0.0f;
                fare.TotalAmount = float.TryParse(tokens[10], out result) ? result : 0.0f;
                return fare;
            }
            catch (Exception ex)
            {
                throw new ArgumentException($"Invalid record: {line}", ex);
            }
        }
    }
}