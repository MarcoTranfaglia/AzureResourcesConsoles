using Microsoft.Azure.Documents;
using Microsoft.Azure.Documents.Client;
using Microsoft.Azure.Documents.Linq;
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

        string endpointUri = _config["EndpointUri"].ToString();
        string primaryKey = _config["PrimaryKey"].ToString();
        string dbName = _config["DB"].ToString();

        string handlername = _config["HandlerName"];
        string inputCollection = _config["InputCollection"];
        string outputCollection = _config["OutputCollection"];

        if (string.IsNullOrWhiteSpace(outputCollection))
            outputCollection = inputCollection;

        bool dryRun = _config.GetValue<bool>("DryRun", true);
        int maxItemCount = _config.GetValue<int>("MaxItemCount", 20);
        int maxNumberOfThreads = _config.GetValue<int>("MaxNumberOfThreads", 1);
        OperationType operation = _config.GetValue<OperationType>("OperationType", OperationType.EditDocument);

        using AbstractHandler handler = GetHandlerInstance("UserHandler");
        handler.SetConfig(_config);

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
                new ParallelOptions { MaxDegreeOfParallelism = _config.GetValue<int>("MaxNumberOfThreads") },
                async (document, cancellationToken) =>
                {
                    try
                    {
                        (string partitionKey, string etag) = AbstractHandler.GetAdditionalData(document);


                        switch (operation)
                        {
                            case OperationType.EditDocument:
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

                                break;

                            case OperationType.DeleteDocument:
                                if (!dryRun)
                                {
                                    var item = await client.DeleteDocumentAsync(documentUri: UriFactory.CreateDocumentUri(databaseId: dbName,
                                                                                                                          collectionId: outputCollection,
                                                                                                                          documentId: document.Id),
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

                                    if (item is not null)
                                        throw new Exception($"Cannot delete {document.Id}");
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

    public static AbstractHandler GetHandlerInstance(string handlerName)
    {
        Type t = Type.GetType($"CosmosDocumentsManager.Handlers.{handlerName}");
        return (AbstractHandler)Activator.CreateInstance(t);
    }

    public enum OperationType
    {
        EditDocument,
        DeleteDocument
    }

}
