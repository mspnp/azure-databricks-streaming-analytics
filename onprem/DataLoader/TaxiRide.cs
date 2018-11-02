namespace Taxi
{
    using System;
    using System.Globalization;
    using Newtonsoft.Json;
    using Newtonsoft.Json.Serialization;

    [JsonObject(NamingStrategyType = typeof(CamelCaseNamingStrategy))]
    public class TaxiRide : TaxiData

    {
        public TaxiRide()
        {
        }

        [JsonProperty]
        public int RateCode { get; set; }

        [JsonProperty]
        public string StoreAndForwardFlag { get; set; }

        [JsonProperty]
        public DateTimeOffset DropoffTime { get; set; }

        [JsonProperty]
        public int PassengerCount { get; set; }

        [JsonProperty]
        public float TripTimeInSeconds { get; set; }

        [JsonProperty]
        public float TripDistanceInMiles { get; set; }

        [JsonProperty]
        public float PickupLon { get; set; }

        [JsonProperty]
        public float PickupLat { get; set; }

        [JsonProperty]
        public float DropoffLon { get; set; }

        [JsonProperty]
        public float DropoffLat { get; set; }

        public static TaxiRide FromString(string line,string header)
        {
            if (string.IsNullOrWhiteSpace(line))
            {
                throw new ArgumentException($"{nameof(line)} cannot be null, empty, or only whitespace");
            }

            string[] tokens = line.Split(',');
            if (tokens.Length != 14)
            {
                throw new ArgumentException($"Invalid record: {line}");
            }

            var ride = new TaxiRide();
            ride.CsvString = line;
            ride.CsvHeader = header;
            try
            {
                ride.Medallion = long.Parse(tokens[0]);
                ride.HackLicense = long.Parse(tokens[1]);
                ride.VendorId = tokens[2];
                ride.RateCode = int.Parse(tokens[3]);
                ride.StoreAndForwardFlag = tokens[4];
                ride.PickupTime = DateTimeOffset.ParseExact(
                    tokens[5], "yyyy-MM-dd HH:mm:ss",
                    CultureInfo.InvariantCulture,
                    DateTimeStyles.AssumeUniversal);
                ride.DropoffTime = DateTimeOffset.ParseExact(
                    tokens[6], "yyyy-MM-dd HH:mm:ss",
                    CultureInfo.InvariantCulture,
                    DateTimeStyles.AssumeUniversal);
                ride.PassengerCount = int.Parse(tokens[7]);
                ride.TripTimeInSeconds = float.Parse(tokens[8]);
                ride.TripDistanceInMiles = float.Parse(tokens[9]);

                ride.PickupLon = float.TryParse(tokens[10], out float result) ? result : 0.0f;
                ride.PickupLat = float.TryParse(tokens[11], out result) ? result : 0.0f;
                ride.DropoffLon = float.TryParse(tokens[12], out result) ? result : 0.0f;
                ride.DropoffLat = float.TryParse(tokens[13], out result) ? result : 0.0f;
                return ride;
            }
            catch (Exception ex)
            {
                throw new ArgumentException($"Invalid record: {line}", ex);
            }
        }
    }
}