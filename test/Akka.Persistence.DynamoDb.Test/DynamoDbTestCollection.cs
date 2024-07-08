using Xunit;

namespace Akka.Persistence.DynamoDb.Test
{
    [CollectionDefinition(Name)]
    public class DynamoDbTestCollection : ICollectionFixture<DynamoDbDatabaseFixture>
    {
        public const string Name = "DynamoDb";
    }
}