using Akka.Actor;
using Akka.Configuration;
using Akka.Persistence.DynamoDb.Journal;
using Akka.Persistence.DynamoDb.Snapshot;

namespace Akka.Persistence.DynamoDb
{
    public sealed class DynamoDbPersistence : IExtension
    {
        public DynamoDbPersistence(
            DynamoDbJournalSettings journalSettings, 
            DynamoDbSnapshotStoreSettings snapshotSettings)
        {
            JournalSettings = journalSettings;
            SnapshotSettings = snapshotSettings;
        }

        public DynamoDbJournalSettings JournalSettings { get; }

        public DynamoDbSnapshotStoreSettings SnapshotSettings { get; }
        
        public static Config DefaultConfig =>
            ConfigurationFactory.FromResource<DynamoDbPersistence>("Akka.Persistence.DynamoDb.reference.conf");
        
        public static DynamoDbPersistence Get(ActorSystem system)
        {
            return system.WithExtension<DynamoDbPersistence, DynamoDbPersistenceProvider>();
        }
    }
}