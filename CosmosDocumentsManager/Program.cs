using CosmosDocumentsManager.Handlers;
using CosmosDocumentsManager.Models;
using Microsoft.Azure.Cosmos;
using Microsoft.Extensions.Configuration;
using System.Diagnostics;

namespace CosmosDocumentsManager;

internal class Program
{
    private static IConfiguration _config { get; set; }
    static async Task Main(string[] args)
    {
        _config = new ConfigurationBuilder()
            .AddJsonFile("appsettings.json", true, true)
            .Build();

        Stopwatch stopwatch = new Stopwatch();
        stopwatch.Start();

        string accountEndpoint = _config["AccountEndpoint"].ToString();
        string databaseName = _config["DatabaseName"].ToString();

        string inputCollection = _config["InputCollection"];
        string outputCollection = _config["OutputCollection"];

        if (string.IsNullOrWhiteSpace(outputCollection))
            outputCollection = inputCollection;

        bool dryRun = _config.GetValue<bool>("DryRun", true);
        int maxItemCount = _config.GetValue<int>("MaxItemCount", 20);
        int maxNumberOfThreads = _config.GetValue<int>("MaxNumberOfThreads", 1);
        OperationType operation = _config.GetValue<OperationType>("OperationType", OperationType.EditDocuments);

        TimesheetHandler handler = new TimesheetHandler();
        handler.InputCollection = inputCollection;

        CosmosClient client = new(connectionString: accountEndpoint);
        Database database = client.GetDatabase(databaseName);
        Container container = database.GetContainer(inputCollection);

        FeedIterator<Timesheet> query = container.GetItemQueryIterator<Timesheet>(
            queryDefinition: new QueryDefinition(handler.GetQuery()),
            requestOptions: new QueryRequestOptions
            {
                MaxItemCount = maxItemCount
            });

        int documentWrites = 0, totalDocuments = 0;

        Console.WriteLine("BEGIN PROCESSING");
        Console.WriteLine($"Operation: {operation}");
        Console.WriteLine($"DryRun: {dryRun}");
        Console.WriteLine($"Collection: {inputCollection}");
        Console.WriteLine($"Query: {handler.GetQuery()}");

        while (query.HasMoreResults)
        {
            FeedResponse<Timesheet> documents = await query.ReadNextAsync();
            Console.WriteLine($"Number of documents: {documents.Count}");

            await Parallel.ForEachAsync(
                documents,
                new ParallelOptions { MaxDegreeOfParallelism = maxNumberOfThreads },
                async (document, cancellationToken) =>
                {
                    try
                    {
                        switch (operation)
                        {
                            case OperationType.EditDocuments:
                                CosmosItem editedDoc = handler.ManageDocument(document);

                                if (editedDoc != null)
                                {
                                    if (!dryRun)
                                    {
                                        // If there is a new partition key we have to delete the old and add a new one
                                        if (editedDoc.PartitionKey != document.PartitionKey)
                                        {
                                            await container.DeleteItemAsync<CosmosItem>(document.Id.ToString(), new PartitionKey(document.PartitionKey));

                                            await container.UpsertItemAsync(editedDoc, new PartitionKey(editedDoc.PartitionKey));
                                        }
                                        else
                                        {
                                            await container.UpsertItemAsync(editedDoc, new PartitionKey(editedDoc.PartitionKey));
                                        }
                                    }

                                    Interlocked.Increment(ref documentWrites);
                                }

                                break;

                            case OperationType.DeleteDocuments:
                                if (!dryRun)
                                {
                                    await container.DeleteItemAsync<CosmosItem>(document.Id.ToString(), new PartitionKey(document.PartitionKey));
                                }

                                break;

                            default:
                                throw new Exception("Operation _configured does not belong to known operations");
                        }

                    }
                    catch (Exception e)
                    {
                        string errorMessage = $"ERROR in processing: {document.Id} in handler {handler.GetType().Name}: {e.Message}";

                        Console.ForegroundColor = ConsoleColor.Red;
                        Console.WriteLine(errorMessage);

                        Console.ResetColor();
                    }
                });

            totalDocuments += documents.Count;
            Console.WriteLine($"Processed a total of {totalDocuments} documents for collection {inputCollection}. Generated a total of {documentWrites} writes on {outputCollection}. Elapsed: {stopwatch.Elapsed}");
        }

        stopwatch.Stop();

        Console.WriteLine($"PROCESSING TERMINATED: {stopwatch.Elapsed}.");
        Console.WriteLine();
    }


    public enum OperationType
    {
        EditDocuments,
        DeleteDocuments
    }


}
