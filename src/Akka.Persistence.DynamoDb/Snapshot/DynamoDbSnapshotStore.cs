using Akka.Actor;
using Akka.Configuration;
using Akka.Event;
using Akka.Persistence.Snapshot;
using Amazon.DynamoDBv2;
using Amazon.DynamoDBv2.DocumentModel;
using Amazon.DynamoDBv2.Model;
using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Linq;
using System.Threading.Tasks;

namespace Akka.Persistence.DynamoDb.Snapshot
{
    public class DynamoDbSnapshotStore : SnapshotStore, IWithUnboundedStash
    {
        public static class Events
        {
            public sealed class Initialized
            {
                public static readonly Initialized Instance = new();
                private Initialized() { }
            }
        }

        private readonly ActorSystem _actorSystem;
        private readonly AmazonDynamoDBClient _client;
        private readonly DynamoDbSnapshotStoreSettings _settings;
        private readonly ILoggingAdapter _log = Context.GetLogger();

        private Table? _table;

        public DynamoDbSnapshotStore(Config? config = null)
        {
            _actorSystem = Context.System;

            _settings = config is null ?
                DynamoDbPersistence.Get(Context.System).SnapshotSettings :
                DynamoDbSnapshotStoreSettings.Create(config);

            _client = DynamoDbSetup.InitClient(_settings);
        }

        protected override void PreStart()
        {
            base.PreStart();

            if (!_settings.AutoInitialize)
            {
                _table = Table.LoadTable(_client, _settings.TableName);

                return;
            }

            Initialize().PipeTo(Self);
            BecomeStacked(WaitingForInitialization);
        }

        public IStash? Stash { get; set; }

        protected override async Task<SelectedSnapshot?> LoadAsync(
            string persistenceId,
            SnapshotSelectionCriteria criteria)
        {
            var filter = new QueryFilter();
            filter.AddCondition(SnapshotDocument.Keys.PersistenceId, QueryOperator.Equal, persistenceId);
            filter.AddCondition(SnapshotDocument.Keys.SequenceNumber, QueryOperator.Between, criteria.MinSequenceNr, criteria.MaxSequenceNr);

            var search = _table!.Query(new QueryOperationConfig
            {
                BackwardSearch = true,
                CollectResults = false,
                Filter = filter,
                ConsistentRead = true
            });

            while (!search.IsDone)
            {
                var document = (await search.GetNextSetAsync())
                    .Select(x => new SnapshotDocument(x))
                    .FirstOrDefault(x => x.Timestamp >= (criteria.MinTimestamp ?? DateTime.MinValue).Ticks &&
                                         x.Timestamp <= criteria.MaxTimeStamp.Ticks);

                if (document != null)
                    return document.ToSelectedSnapshot(_actorSystem);
            }

            return null;
        }

        protected override async Task SaveAsync(SnapshotMetadata metadata, object snapshot)
        {
            await _table!.PutItemAsync(SnapshotDocument.ToDocument(metadata, snapshot, _actorSystem));
        }

        protected override async Task DeleteAsync(SnapshotMetadata metadata)
        {
            var document = await _table!.GetItemAsync(metadata.PersistenceId, metadata.SequenceNr);

            await _table.DeleteItemAsync(document);
        }

        protected override async Task DeleteAsync(string persistenceId, SnapshotSelectionCriteria criteria)
        {
            var filter = new QueryFilter();
            filter.AddCondition(SnapshotDocument.Keys.PersistenceId, QueryOperator.Equal, persistenceId);
            filter.AddCondition(SnapshotDocument.Keys.SequenceNumber, QueryOperator.Between, criteria.MinSequenceNr, criteria.MaxSequenceNr);

            var search = _table!.Query(persistenceId, filter);

            while (!search.IsDone)
            {
                var items = await search.GetNextSetAsync();

                var batch = _table.CreateBatchWrite();

                foreach (var item in items)
                {
                    var snapshotDocument = new SnapshotDocument(item);

                    if (snapshotDocument.Timestamp >= (criteria.MinTimestamp ?? DateTime.MinValue).Ticks &&
                        snapshotDocument.Timestamp <= criteria.MaxTimeStamp.Ticks)
                    {
                        batch.AddItemToDelete(item);
                    }
                }

                await batch.ExecuteAsync();
            }
        }

        private async Task<object> Initialize()
        {
            try
            {
                await _client.EnsureTableExistsWithDefinition(
                    _settings.TableName,
                    new List<AttributeDefinition>
                    {
                        new(SnapshotDocument.Keys.PersistenceId, ScalarAttributeType.S),
                        new(SnapshotDocument.Keys.SequenceNumber, ScalarAttributeType.N)
                    }.ToImmutableList(),
                    new List<KeySchemaElement>
                    {
                        new(SnapshotDocument.Keys.PersistenceId, KeyType.HASH),
                        new(SnapshotDocument.Keys.SequenceNumber, KeyType.RANGE)
                    }.ToImmutableList(),
                    ImmutableList<GlobalSecondaryIndex>.Empty);

                _table = Table.LoadTable(_client, _settings.TableName);

                return Events.Initialized.Instance;
            }
            catch (Exception e)
            {
                return new Status.Failure(e);
            }
        }

        private bool WaitingForInitialization(object message)
        {
            switch (message)
            {
                case Events.Initialized:
                    UnbecomeStacked();
                    Stash?.UnstashAll();
                    return true;

                case Status.Failure failure:
                    _log.Error(failure.Cause, "Error during snapshot store initialization");
                    Context.Stop(Self);
                    return true;

                //TODO: Remove once the obsolete Failure is removed
                case Failure failure:
                    _log.Error(failure.Exception, "Error during snapshot store initialization");
                    Context.Stop(Self);
                    return true;

                default:
                    Stash?.Stash();
                    return true;
            }
        }
    }
}