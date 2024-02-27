using Azure.Identity;
using Microsoft.Extensions.Configuration;
using Microsoft.Graph;
using Microsoft.Graph.Models;
using System;

namespace CheckAdb2cUser;

internal class Program
{
    static void Main(string[] args)
    {
        IConfiguration config = new ConfigurationBuilder()
            .AddJsonFile(@"appsettings.json", true, true)
            .Build();

        string applicationClientId = config["IdentityProvider:ClientId"];
        string secretId = config["IdentityProvider:ClientSecret"];
        string tenantId = config["IdentityProvider:Tenant"];

        ClientSecretCredential credentials = new ClientSecretCredential(
        tenantId, applicationClientId, secretId,
            new TokenCredentialOptions
            {
                AuthorityHost =
            AzureAuthorityHosts.AzurePublicCloud
            });

        GraphServiceClient graphServiceClient = new GraphServiceClient(credentials);

        string phoneNumberToSearch = "+39.1111110232";

        var user = graphServiceClient.Users
            [phoneNumberToSearch]
            .GetAsync()
            .GetAwaiter()
            .GetResult();

        PrintUser(user);

        Console.ReadLine();
    }

    private static void PrintUser(User user)
    {
        if (user == null)
        {
            Console.WriteLine("User does not exist");

            return;
        }

        Console.WriteLine($"Id: {user.Id}");
        Console.WriteLine($"DisplayName: {user.DisplayName}");

        foreach (var identity in user.Identities)
            Console.WriteLine($"Identity: Issuer: {identity.Issuer}, SignInType: {identity.SignInType}, IssuerAssignedId: {identity.IssuerAssignedId}");

        Console.WriteLine($"CreatedDateTime: {user.CreatedDateTime}");
    }
}
