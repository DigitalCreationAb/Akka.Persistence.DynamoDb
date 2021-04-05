using Xunit;

namespace Akka.Persistence.DynamoDb.Test
{
    [CollectionDefinition(Name)]
    public class DynamoDbTestCollection : ICollectionFixture<DynamoDbTestCollection.Fixture>
    {
        public const string Name = "DynamoDb";
        
        public class Fixture : LocalstackDynamoDbFixture
        {
            
        }
    }
}