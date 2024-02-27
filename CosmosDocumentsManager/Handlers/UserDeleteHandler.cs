using Microsoft.Azure.Documents;

namespace CosmosDocumentsManager.Handlers;

public class UserDeleteHandler : AbstractHandler
{
    public override string GetQuery() => "SELECT * FROM c where c._type = 'MT.Users.B2C";

    public override Document ManageDocument(Document d)
    {
        throw new NotImplementedException();
    }
}
