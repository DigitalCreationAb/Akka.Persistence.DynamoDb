using Akka.Persistence.DynamoDb.Query;
using Akka.Persistence.Query;
using Akka.Persistence.TCK.Query;
using Xunit;

namespace Akka.Persistence.DynamoDb.Test.Query
{
    [Collection(DynamoDbTestCollection.Name)]
    public class DynamoDbEventsByTagSpec : EventsByTagSpec
    {
        public DynamoDbEventsByTagSpec(DynamoDbTestCollection.Fixture fixture)
            : base(DynamoDbStorageConfigHelper.DynamoDbConfig(fixture))
        {
            DynamoDbPersistence.Get(Sys);
            
            ReadJournal = Sys.ReadJournalFor<DynamoDbReadJournal>(DynamoDbReadJournal.Identifier);
        }
    }
}