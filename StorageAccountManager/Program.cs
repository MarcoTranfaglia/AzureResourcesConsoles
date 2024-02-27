using Azure.Storage.Blobs;
using Azure.Storage.Blobs.Models;
using Microsoft.Extensions.Configuration;
using System.Diagnostics;

namespace CheckAdb2cUser;

internal class Program
{
    static async Task Main(string[] args)
    {
        IConfiguration config = new ConfigurationBuilder()
            .AddJsonFile(@"appsettings.json", true, true)
            .Build();

        string connectionString = config["StorageAccount:ConnectionString"];
        string containerName = config["StorageAccount:ContainerName"];
        string fullUriWithSas = config["StorageAccount:FullUriSas"];

        if (string.IsNullOrEmpty(fullUriWithSas))
        {
            Debug.Assert(connectionString == null);
            Debug.Assert(containerName == null);
        }
        else
        {
            Debug.Assert(Uri.IsWellFormedUriString(fullUriWithSas, UriKind.Absolute));
        }

        BlobServiceClient serviceClient = new BlobServiceClient(connectionString);

        BlobContainerClient containerClient = await serviceClient.CreateBlobContainerAsync(containerName);

        string operation = config["Operation"];

        switch (operation)
        {
            case "CheckAccess":
                if (fullUriWithSas is not null)
                    await TestSasAccessAsync(fullUriWithSas, testWriteAndDelete: true, testList: true);
                else
                    await TestAccessAsync(containerClient, testWriteAndDelete: true, testList: true);

                break;

            case "DownloadAllBlobsFromContainer":
                await DownloadAllFilesAsync(containerClient, containerName);
                break;

            default:
                throw new Exception("No valid operation selected");
        }

        if (fullUriWithSas is not null)
            await TestSasAccessAsync(fullUriWithSas, testWriteAndDelete: true, testList: true);
        else
            await TestAccessAsync(containerClient, testWriteAndDelete: true, testList: true);


        Console.ReadLine();
    }


    private static async Task DownloadAllFilesAsync(BlobContainerClient containerClient, string containerName)
    {
        try
        {
            var blobs = containerClient.GetBlobs();

            foreach (var blob in blobs)
            {
                var blobClient = containerClient.GetBlobClient(blob.Name);
                await blobClient.DownloadToAsync($@".\output\{blob.Name}");

            }
        }
        catch (Exception ex)
        {
            throw new Exception("Error downloading blob file", ex);
        }
    }

    private static async Task TestSasAccessAsync(String uri, bool testWriteAndDelete, bool testList)
    {
        try
        {
            BlobContainerClient containerClient = new BlobContainerClient(new Uri(uri));


            if (testList)
            {
                foreach (var blob in containerClient.GetBlobs())
                {
                    Console.WriteLine(blob.Name);
                }
            }

            if (testWriteAndDelete)
            {
                string blobName = "test.txt";
                string blobContent = "Hello world";
                BlobContentInfo blobInfo = await containerClient.UploadBlobAsync(blobName, new BinaryData(blobContent));
                await containerClient.DeleteBlobAsync(blobName);
            }
        }
        catch (Exception ex)
        {
            throw new Exception("Failed to validate SAS Uri: " + ex.Message, ex);
        }
    }

    private static async Task TestAccessAsync(BlobContainerClient containerClient, bool testWriteAndDelete, bool testList)
    {
        try
        {
            if (testList)
            {
                foreach (var blob in containerClient.GetBlobs())
                {
                    Console.WriteLine(blob.Name);
                }
            }

            if (testWriteAndDelete)
            {
                string blobName = "test.txt";
                string blobContent = "Hello world";
                BlobContentInfo blobInfo = await containerClient.UploadBlobAsync(blobName, new BinaryData(blobContent));
                await containerClient.DeleteBlobAsync(blobName);
            }
        }
        catch (Exception ex)
        {
            throw new Exception("Failed to validate SAS Uri: " + ex.Message, ex);
        }


    }
}
