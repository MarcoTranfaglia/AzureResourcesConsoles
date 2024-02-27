using Microsoft.Azure.Documents;

namespace CosmosDocumentsManager.Handlers;

public class ConcreteHandler : AbstractHandler
{
    public override string GetQuery() => "SELECT * FROM c";

    public override Document ManageDocument(Document d)
    {
        // Operations on all Users
        Guid userId = d.GetPropertyValue<Guid>("userId");

        //remove property
        d.SetPropertyValue("userId", null);

        d.SetPropertyValue("userIdentifier", userId);

        string type = d.GetPropertyValue<string>("_type");

        switch (type)
        {
            case "MT.Users.B2C":

                bool closed = d.GetPropertyValue<bool?>("closed") ?? false;
                if (closed)
                    d.SetPropertyValue("status", "Closed");

                break;

            case "MT.Users.B2B":

                string status = d.GetPropertyValue<string>("status");

                if (status == "WaitingForClosure")
                    d.SetPropertyValue("status", "Suspended");

                break;
            default:
                return null;
        }

        return d;
    }

}
