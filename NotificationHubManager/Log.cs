namespace NotificationHubManager;
public static class Log
{
    public static bool LogEnabled;

    private static string Now => DateTime.UtcNow.ToString(@"yyyy-MM-ddTHH:mm:ss.fffZ");

    public static void Success(string message)
    {
        Console.ForegroundColor = ConsoleColor.Green;
        PrintLog("SUCC", message);
        Console.ResetColor();
    }

    public static void Info(string message)
    {
        PrintLog("INFO", message);
    }

    public static void Warning(string message)
    {
        Console.ForegroundColor = ConsoleColor.Yellow;
        PrintLog("WARN", message);
        Console.ResetColor();
    }

    public static void Error(string message, Exception exception = null)
    {
        Console.ForegroundColor = ConsoleColor.Red;
        PrintLog("ERR", message, exception);
        Console.ResetColor();
    }

    private static void PrintLog(string level, string message, Exception exception = null)
    {
        if (!LogEnabled)
            return;

        string logString = $"{Now} {level} - {message}";

        Console.Out.WriteLine(logString);

        if (exception is { })
        {
            if (exception is AggregateException aggregateException)
            {
                foreach (var ex in aggregateException.Flatten().InnerExceptions)
                {
                    Console.Out.WriteLine(ex.ToLog());
                }
            }
            else
            {
                Console.Out.WriteLine(exception.ToLog());
            }
        }

        Console.Out.Flush();
    }

    private static string ToLog(this Exception ex)
    {
        return $"[{ex.GetType().Name}] {ex.Message} {Environment.NewLine}{ex.StackTrace} {Environment.NewLine}";
    }

}
