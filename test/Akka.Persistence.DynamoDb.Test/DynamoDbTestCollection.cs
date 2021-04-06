using Xunit;

namespace Akka.Persistence.DynamoDb.Test
{
    [CollectionDefinition(Name)]
    public class DynamoDbTestCollection : ICollectionFixture<LocalstackDynamoDbFixture>
    {
        public const string Name = "DynamoDb";
    }
}