using Microsoft.Azure.Documents;
using Microsoft.Extensions.Configuration;

namespace CosmosDocumentsManager;

public abstract class AbstractHandler : IDisposable
{
    protected IConfiguration _config;
    protected bool _logEnabled;
    private bool _disposedValue;

    public virtual string GetQuery() => null;

    internal void SetConfig(IConfiguration config)
    {
        _config = config;
        OnInit();
    }

    internal virtual void OnInit()
    {
        Log.LogEnabled = _config.GetValue<bool>("LogEnabled");
        _logEnabled = _config.GetValue<bool>("LogEnabled");
    }

    public static (string partitionKey, string etag) GetAdditionalData(Document d)
    {
        return (
            d.GetPropertyValue<string>("partitionKey"),
            d.GetPropertyValue<string>("_etag")
        );
    }

    public abstract Document ManageDocument(Document d);

    protected virtual void Dispose(bool disposing)
    {
        if (!_disposedValue)
        {
            if (disposing)
            {
                DisposeManagedObjects();
            }

            _disposedValue = true;
        }
    }

    public void Dispose()
    {
        // Do not change this code. Put cleanup code in 'Dispose(bool disposing)' method
        Dispose(disposing: true);
        GC.SuppressFinalize(this);
    }

    public virtual void DisposeManagedObjects()
    {

    }
}
