using Akka.Actor;
using Akka.Persistence.DynamoDb.Journal;
using Akka.Persistence.DynamoDb.Snapshot;

namespace Akka.Persistence.DynamoDb
{
    public sealed class DynamoDbPersistenceProvider : ExtensionIdProvider<DynamoDbPersistence>
    {
        public override DynamoDbPersistence CreateExtension(ExtendedActorSystem system)
        {
            system.Settings.InjectTopLevelFallback(DynamoDbPersistence.DefaultConfig);

            var journalSettings =
                DynamoDbJournalSettings.Create(
                    system.Settings.Config.GetConfig("akka.persistence.journal.dynamodb"));

            var snapshotSettings =
                DynamoDbSnapshotStoreSettings.Create(
                    system.Settings.Config.GetConfig("akka.persistence.snapshot-store.dynamodb"));

            return new DynamoDbPersistence(journalSettings, snapshotSettings);
        }
    }}