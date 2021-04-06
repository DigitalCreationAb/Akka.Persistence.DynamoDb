using Akka.Persistence.TCK.Snapshot;
using Xunit;

namespace Akka.Persistence.DynamoDb.Test.Snapshot
{
    [Collection(DynamoDbTestCollection.Name)]
    public class DynamoDbSnapshotStoreSpec : SnapshotStoreSpec
    {
        public DynamoDbSnapshotStoreSpec(LocalstackDynamoDbFixture fixture)
            : base(DynamoDbStorageConfigHelper.DynamoDbConfig(fixture))
        {
            DynamoDbPersistence.Get(Sys);
            Initialize();
        }

        protected override bool SupportsSerialization { get; } = false;
    }
}