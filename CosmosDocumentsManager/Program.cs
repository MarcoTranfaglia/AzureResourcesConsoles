using Microsoft.Azure.Documents;
using Microsoft.Azure.Documents.Client;
using Microsoft.Azure.Documents.Linq;
using Microsoft.Extensions.Configuration;
using System.Diagnostics;

namespace CosmosDocumentsManager;

internal class Program
{
    static async Task Main(string[] args)
    {
        IConfiguration config = new ConfigurationBuilder()
            .AddJsonFile("appsettings.json", true, true)
            .Build();

        Stopwatch stopwatch = new Stopwatch();
        stopwatch.Start();

        //
        // Tier configs
        //
        string endpointUri = config["EndpointUri"].ToString();
        string primaryKey = config["PrimaryKey"].ToString();
        string dbName = config["DB"].ToString();

        string handlername = config["HandlerName"];
        string inputCollection = config["InputCollection"];
        string outputCollection = config["OutputCollection"];

        if (string.IsNullOrWhiteSpace(outputCollection))
            outputCollection = inputCollection;

        bool dryRun = config.GetValue<bool>("DryRun", true);
        int maxItemCount = config.GetValue<int>("MaxItemCount", 20);
        int maxNumberOfThreads = config.GetValue<int>("MaxNumberOfThreads", 1);

        using AbstractHandler handler = GetHandlerInstance("ConcreteHandler");
        handler.SetConfig(config);

        DocumentClient client = new DocumentClient(new Uri(endpointUri), primaryKey);

        IDocumentQuery<Document> query = client.CreateDocumentQuery<Document>(
            UriFactory.CreateDocumentCollectionUri(dbName, inputCollection),
            handler.GetQuery(),
            new FeedOptions
            {
                MaxItemCount = maxItemCount,
                MaxDegreeOfParallelism = -1,
                EnableCrossPartitionQuery = true,
                PopulateQueryMetrics = true
            })
            .AsDocumentQuery();

        int documentWrites = 0, totalDocuments = 0;

        if (dryRun)
            Console.WriteLine("DRY RUN");

        Console.WriteLine("BEGIN PROCESSING");
        while (query.HasMoreResults)
        {
            Console.WriteLine("Quering");
            FeedResponse<Document> documents = await query.ExecuteNextAsync<Document>();
            Console.WriteLine("End query");

            await Parallel.ForEachAsync(
                documents,
                new ParallelOptions { MaxDegreeOfParallelism = config.GetValue<int>("MaxNumberOfThreads") },
                async (document, cancellationToken) =>
                {
                    try
                    {
                        (string partitionKey, string etag) = AbstractHandler.GetAdditionalData(document);
                        Document editedDoc = handler.ManageDocument(document);

                        if (editedDoc != null)
                        {
                            if (!dryRun)
                            {
                                await client.ReplaceDocumentAsync(documentUri: UriFactory.CreateDocumentUri(databaseId: dbName,
                                                                                                            collectionId: outputCollection,
                                                                                                            documentId: editedDoc.Id),
                                                                  document: editedDoc,
                                                                  options: new RequestOptions
                                                                  {
                                                                      PartitionKey = new PartitionKey(partitionKey),
                                                                      AccessCondition = new AccessCondition
                                                                      {
                                                                          Condition = etag,
                                                                          Type = AccessConditionType.IfMatch
                                                                      }
                                                                  },
                                                                  cancellationToken: cancellationToken);
                            }

                            Interlocked.Increment(ref documentWrites);
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

    public static AbstractHandler GetHandlerInstance(string handlerName)
    {
        Type t = Type.GetType($"CosmosDocumentsManager.Handlers.{handlerName}");
        return (AbstractHandler)Activator.CreateInstance(t);
    }

}
