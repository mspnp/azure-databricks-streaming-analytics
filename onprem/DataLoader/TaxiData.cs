namespace Taxi
{
    using System;
    using System.Globalization;
    using Newtonsoft.Json;
    using Newtonsoft.Json.Serialization;

    [JsonObject(NamingStrategyType = typeof(CamelCaseNamingStrategy))]
    public abstract class TaxiData
    {
        public TaxiData()
        {
        }

        [JsonProperty]
        public long Medallion { get; set; }

        [JsonProperty]
        public long HackLicense { get; set; }

        [JsonProperty]
        public string VendorId { get; set; }

        [JsonProperty]
        public DateTimeOffset PickupTime { get; set; }

        [JsonIgnore]
        public string PartitionKey
        {
            get => $"{Medallion}_{HackLicense}_{VendorId}";
        }

        [JsonIgnore]
        protected string CsvHeader { get; set; }  


        [JsonIgnore]
        protected string CsvString { get; set; }

        public string GetData(DataFormat dataFormat)
        {
            if (dataFormat == DataFormat.Csv)
            {
                return $"{CsvHeader}\r\n{CsvString}";
            }
            else if (dataFormat == DataFormat.Json)
            {
                return JsonConvert.SerializeObject(this);
            }
            else
            {
                throw new ArgumentException($"Invalid DataFormat: {dataFormat}");
            }
        }
    }
}