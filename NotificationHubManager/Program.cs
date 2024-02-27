using Microsoft.Azure.NotificationHubs;
using Microsoft.Extensions.Configuration;

namespace NotificationHubManager;

internal class Program
{
    private static IConfiguration _config { get; set; }
    static async Task Main(string[] args)
    {
        _config = new ConfigurationBuilder()
            .AddJsonFile("appsettings.json", true, true)
            .Build();

        NotificationHubClient hub =
            NotificationHubClient.CreateClientFromConnectionString(
                _config.GetValue<string>("ConnectionString"),
                _config.GetValue<string>("HubName"));

        OperationType operation = _config.GetValue<OperationType>("OperationType");

        switch (operation)
        {
            case OperationType.RemoveInstallation:
                await RemoveInstallations(hub);
                break;

            case OperationType.AddInstallation:
                throw new NotImplementedException();
                break;

            default: throw new Exception("Operation type not existing");
        }


        Log.Info("Finished!");
    }

    private static async Task RemoveInstallations(NotificationHubClient hub)
    {
        Log.Info("Starting NotificationHubCleanup");

        string? sourceFile = _config.GetValue<string>("SourceFile");
        List<string> deviceIds = (await File.ReadAllLinesAsync(sourceFile))
            .ToList();

        int workers = _config.GetValue<int>("Workers");
        IEnumerable<string[]> chunks = deviceIds.Chunk(deviceIds.Count / workers);

        int exceptionCount = 0;

        IEnumerable<Task> tasks = chunks.Select(chunk =>
            Task.Run(async () =>
            {
                Log.Info("Starting worker...");

                int delayMs = _config.GetValue<int>("DelayBetweenCycles");

                foreach (string deviceId in chunk)
                {
                    try
                    {
                        await hub.DeleteInstallationAsync(deviceId);

                        lock (deviceIds)
                            deviceIds.Remove(deviceId);

                        await Task.Delay(delayMs);
                    }
                    catch (Exception e)
                    {
                        Log.Warning($"Device {deviceId} not removed with exception {e.StackTrace}");

                        if (Interlocked.Increment(ref exceptionCount) >
                            _config.GetValue<int>("MaxExceptionsBeforeExit"))
                            return;
                    }

                    await Task.Delay(TimeSpan.FromMilliseconds(delayMs));
                }
            }));

        await Task.WhenAll(tasks);

        Log.Info("Writing remaining devices to file...");

        await File.WriteAllLinesAsync(sourceFile, deviceIds);
    }


    public enum OperationType
    {
        AddInstallation,
        RemoveInstallation
    }

}
