using Azure.Identity;
using Azure.Messaging.ServiceBus;
using Microsoft.Extensions.Configuration;
using System.Text.RegularExpressions;

namespace ServiceBusManager;
internal class Program
{
    private static IConfiguration _config { get; set; }
    static async Task Main(string[] args)
    {
        _config = new ConfigurationBuilder()
            .AddJsonFile("appsettings.json", true, true)
            .Build();

        var jsonMessages = await CreateJsonMessagesAsync();

        Log.Info($"Parsed {jsonMessages.Count} entries from provided {_config["SourceDataFilePath"]} file...");

        await SendMessagesAsync(jsonMessages);

        Log.Info("Finished!");
    }

    internal static async Task<List<(string Id, string Message)>> CreateJsonMessagesAsync()
    {
        string messageTemplateFilePath = _config["MessageTemplateFilePath"];
        string sourceDataFilePath = _config["SourceDataFilePath"];

        var jsonMessages = new List<(string Id, string Message)>();
        using var payloadReader = new StreamReader(messageTemplateFilePath);
        string jsonTemplate = payloadReader.ReadToEnd();

        using var csvReader = new StreamReader(sourceDataFilePath);

        string csvData = await csvReader.ReadToEndAsync();
        string[] csvEntries = csvData.Split(Environment.NewLine, StringSplitOptions.RemoveEmptyEntries);

        foreach (string line in csvEntries)
        {
            string json = jsonTemplate;
            string[] @params = line.Split(',');
            for (int i = 0; i < @params.Length; i++)
            {
                string placeholder = $"#p{i + 1}";

                json = Regex.Replace(json, placeholder + @"\b", @params[i]);
            }

            string userId = @params[0];
            json = json.Replace("<userId>", userId);
            json = json.Replace("<date>", DateTime.UtcNow.ToString("u"));
            jsonMessages.Add((userId, json));
        }

        return jsonMessages;
    }

    private static async Task SendMessagesAsync(List<(string Id, string Message)> jsonMessages)
    {
        ServiceBusClient client;
        ServiceBusSender sender;

        string serviceBusConnectionString = _config["ServiceBusConnectionString"];
        string serviceBusFullNamespace = _config["ServiceBusFullNamespace"];

        string messageSubject = _config["MessageSubject"]; // aka Label
        string topicName = _config["TopicName"];
        string targetSubscription = _config["TargetSubscription"];

        // Set the transport type to AmqpWebSockets so that the ServiceBusClient uses the port 443. 
        // If you use the default AmqpTcp, ensure that ports 5671 and 5672 are open.
        var clientOptions = new ServiceBusClientOptions
        {
            TransportType = ServiceBusTransportType.AmqpWebSockets
        };

        // Use Managed Identities
        if (!string.IsNullOrEmpty(serviceBusFullNamespace))
            client = new ServiceBusClient(serviceBusFullNamespace, new DefaultAzureCredential(), clientOptions);
        else
            client = new ServiceBusClient(serviceBusConnectionString);

        try
        {
            sender = client.CreateSender(topicName);
        }
        catch (Exception e)
        {
            Log.Error("Error connecting to Azure Service Bus, please check config file.", e);
            return;
        }

        Log.Info($"Submitting {jsonMessages.Count} messages on {topicName} topic with Label: {messageSubject}...");

        int messagesProcessedCount = 0;

        using ServiceBusMessageBatch messageBatch = await sender.CreateMessageBatchAsync();

        foreach ((string Id, string Message) msg in jsonMessages)
        {
            Log.Info($"{messagesProcessedCount++:D4}-Send message with Label:{messageSubject} for user: {msg.Id}");

            ServiceBusMessage message = new ServiceBusMessage();
            message.Subject = messageSubject;
            message.MessageId = Guid.NewGuid().ToString();
            message.ApplicationProperties.Add("UserId", msg.Id);
            message.ApplicationProperties.Add("CreationDate", DateTime.UtcNow.ToString("O"));

            if (!string.IsNullOrWhiteSpace(targetSubscription))
                message.ApplicationProperties.Add("TargetSubscription", targetSubscription);
        }

        try
        {
            await sender.SendMessagesAsync(messageBatch);
            Log.Info($"A batch of {messagesProcessedCount} messages has been published to the queue.");
        }
        catch (Exception ex)
        {
            Log.Error($"{ex.Message}");
            Console.ReadLine();
        }

        await sender.CloseAsync();

        Log.Info("Processing completed.");
    }
}
