using Newtonsoft.Json;

namespace CosmosDocumentsManager.Models;

public class CosmosItem : IDisposable
{
    [JsonProperty("partitionKey")]
    public string PartitionKey { get; set; }

    [JsonProperty("id")]
    public Guid Id { get; set; }

    [JsonProperty("_etag")]
    public string Etag { get; set; }

    public void Dispose()
    {

    }
}