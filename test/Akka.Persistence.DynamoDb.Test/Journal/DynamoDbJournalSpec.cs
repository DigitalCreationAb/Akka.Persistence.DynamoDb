using Akka.Persistence.TCK.Journal;
using Xunit;

namespace Akka.Persistence.DynamoDb.Test.Journal
{
    [Collection(DynamoDbTestCollection.Name)]
    public class DynamoDbJournalSpec : JournalSpec
    {
        public DynamoDbJournalSpec(LocalstackDynamoDbFixture fixture)
            : base(DynamoDbStorageConfigHelper.DynamoDbConfig(fixture))
        {
            DynamoDbPersistence.Get(Sys);
            Initialize();
        }

        protected override bool SupportsSerialization { get; } = false;

        protected override bool SupportsRejectingNonSerializableObjects { get; } = false;
    }
}