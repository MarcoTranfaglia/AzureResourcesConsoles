using CosmosDocumentsManager.Models;
using Newtonsoft.Json;

namespace CosmosDocumentsManager.Handlers;

public class TimesheetHandler
{
    public string InputCollection { get; set; }

    public string GetQuery() => $"SELECT * FROM {InputCollection} c WHERE c._type = '{Timesheet.Type}'";

    public Timesheet ManageDocument(Timesheet timesheet)
    {
        Guid customerIdToSet;

        if (timesheet.Customer == "Repower")
            customerIdToSet = Guid.Parse("c45c20e2-6a3b-43ae-b88d-6a941e7b808d");

        else if (timesheet.Customer == "Net Informatica")
            customerIdToSet = Guid.Parse("c9dd11bd-d990-4010-8386-5411f8d5b9ad");

        else
            throw new ApplicationException($"Customer {timesheet.Customer} non censito");

        timesheet.PartitionKey = customerIdToSet.ToString();
        timesheet.Customer = null;
        timesheet.CustomerId = customerIdToSet;

        return timesheet;
    }
}

public class Timesheet : CosmosItem
{
    [JsonProperty("_type")]
    public static string Type => "MTConsulting.Repository.Models.Timesheet";

    [JsonProperty("customerId", DefaultValueHandling = DefaultValueHandling.Ignore)]
    public Guid? CustomerId { get; set; }

    [JsonProperty("customer", DefaultValueHandling = DefaultValueHandling.Ignore)] // So it will be deleted when set to null
    public string Customer { get; set; }

    [JsonProperty("jobOrder")]
    public string JobOrder { get; set; }

    [JsonProperty("description")]
    public string Description { get; set; }

    [JsonProperty("hours")]
    public float Hours { get; set; }

    [JsonProperty("date")]
    public DateOnly Date { get; set; }

}

