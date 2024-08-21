using Akka.Actor;
using Akka.Configuration;
using Akka.Event;
using Akka.Persistence.DynamoDb.Query.QueryApi;
using Akka.Persistence.Journal;
using Akka.Util.Internal;
using Amazon.DynamoDBv2;
using Amazon.DynamoDBv2.DocumentModel;
using Amazon.DynamoDBv2.Model;
using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Linq;
using System.Threading.Tasks;

namespace Akka.Persistence.DynamoDb.Journal
{
    public class DynamoDbJournal : AsyncWriteJournal, IWithUnboundedStash
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
        private readonly DynamoDbJournalSettings _settings;
        private readonly ILoggingAdapter _log = Context.GetLogger();

        private readonly Dictionary<string, ISet<IActorRef>> _tagSubscribers = new();
        private readonly HashSet<IActorRef> _allPersistenceIdSubscribers = new();

        private Table? _table;

        public DynamoDbJournal(Config? config = null)
        {
            _actorSystem = Context.System;

            _settings = config is null ?
                DynamoDbPersistence.Get(Context.System).JournalSettings :
                DynamoDbJournalSettings.Create(config);

            _client = DynamoDbSetup.InitClient(_settings);
        }

        public IStash? Stash { get; set; }

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

        public override async Task ReplayMessagesAsync(
            IActorContext context,
            string persistenceId,
            long fromSequenceNr,
            long toSequenceNr,
            long max,
            Action<IPersistentRepresentation> recoveryCallback)
        {
            var filter = new QueryFilter(EventDocument.Keys.SequenceNumber, QueryOperator.Between, fromSequenceNr, toSequenceNr);

            var search = _table!.Query(EventDocument.GetEventGroupKey(persistenceId), filter);

            var returnedItems = 0L;

            while (!search.IsDone)
            {
                if (returnedItems >= max)
                    break;

                var items = await search.GetNextSetAsync();

                foreach (var item in items)
                {
                    if (returnedItems >= max)
                        break;

                    var eventDocument = new EventDocument(item);

                    recoveryCallback(eventDocument.ToPersistent(_actorSystem));

                    returnedItems++;
                }
            }

            if (returnedItems == 0)
                NotifyNewPersistenceIdAdded(persistenceId);
        }

        public override async Task<long> ReadHighestSequenceNrAsync(string persistenceId, long fromSequenceNr)
        {
            var item = await _table!.GetItemAsync(EventDocument.GetHighestSequenceNumberGroupKey(persistenceId), 0L);

            var eventDocument = item != null ? new EventDocument(item) : null;

            var sequenceNumber = eventDocument?.HighestSequenceNumber ?? 0;

            if (sequenceNumber <= 0)
                NotifyNewPersistenceIdAdded(persistenceId);

            return sequenceNumber;
        }

        protected override async Task<IImmutableList<Exception?>?> WriteMessagesAsync(IEnumerable<AtomicWrite> messages)
        {
            var results = new List<Exception?>();

            var allTags = new List<string>();

            foreach (var atomicWrite in messages)
            {
                try
                {
                    var batch = _table!.CreateBatchWrite();

                    var items = atomicWrite.Payload.AsInstanceOf<IImmutableList<IPersistentRepresentation>>();

                    foreach (var persistentRepresentation in items)
                    {
                        if (persistentRepresentation.SequenceNr == 0)
                            NotifyNewPersistenceIdAdded(persistentRepresentation.PersistenceId);

                        var (documents, tags) = EventDocument.ToDocument(persistentRepresentation, _actorSystem);

                        allTags.AddRange(tags);

                        foreach (var document in documents)
                            batch.AddDocumentToPut(document);
                    }

                    var highestSequenceNumbers = items
                        .GroupBy(x => x.PersistenceId)
                        .Select(x => EventDocument.ToHighestSequenceNumberDocument(
                            x.Key,
                            x
                                .Select(y => y.SequenceNr)
                                .OrderByDescending(y => y)
                                .FirstOrDefault()))
                        .ToImmutableList();

                    foreach (var highestSequenceNumber in highestSequenceNumbers)
                    {
                        await _table.UpdateItemAsync(highestSequenceNumber);
                    }

                    await batch.ExecuteAsync();

                    results.Add(null);
                }
                catch (Exception exception)
                {
                    results.Add(exception);
                }
            }

            var documentTags = allTags.Distinct().ToImmutableList();

            if (_tagSubscribers.Any() && documentTags.Any())
            {
                foreach (var tag in documentTags)
                    NotifyTagChange(tag);
            }

            return results.Any(x => x != null) ? results.ToImmutableList() : null;
        }

        protected override async Task DeleteMessagesToAsync(string persistenceId, long toSequenceNr)
        {
            var filter = new QueryFilter(EventDocument.Keys.SequenceNumber, QueryOperator.LessThanOrEqual, toSequenceNr);

            var search = _table!.Query(EventDocument.GetEventGroupKey(persistenceId), filter);

            while (!search.IsDone)
            {
                var items = await search.GetNextSetAsync();

                var batch = _table.CreateBatchWrite();

                foreach (var item in items)
                    batch.AddItemToDelete(item);

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
                        new(EventDocument.Keys.GroupKey, ScalarAttributeType.S),
                        new(EventDocument.Keys.SequenceNumber, ScalarAttributeType.N),
                        new(EventDocument.Keys.DocumentType, ScalarAttributeType.S),
                        new(EventDocument.Keys.PersistenceId, ScalarAttributeType.S),
                        new(EventDocument.Keys.Tag, ScalarAttributeType.S),
                        new(EventDocument.Keys.Timestamp, ScalarAttributeType.N)
                    }.ToImmutableList(),
                    new List<KeySchemaElement>
                    {
                        new(EventDocument.Keys.GroupKey, KeyType.HASH),
                        new(EventDocument.Keys.SequenceNumber, KeyType.RANGE)
                    }.ToImmutableList(),
                    ImmutableList.Create(new GlobalSecondaryIndex
                    {
                        IndexName = "ByDocumentType",
                        KeySchema = new List<KeySchemaElement>
                            {
                                new(EventDocument.Keys.DocumentType, KeyType.HASH),
                                new(EventDocument.Keys.PersistenceId, KeyType.RANGE)
                            },
                        Projection = new Projection
                        {
                            ProjectionType = ProjectionType.KEYS_ONLY
                        }
                    },
                        new GlobalSecondaryIndex
                        {
                            IndexName = "ByTag",
                            KeySchema = new List<KeySchemaElement>
                            {
                                new(EventDocument.Keys.Tag, KeyType.HASH),
                                new(EventDocument.Keys.Timestamp, KeyType.RANGE)
                            },
                            Projection = new Projection
                            {
                                ProjectionType = ProjectionType.INCLUDE,
                                NonKeyAttributes = new List<string>
                                {
                                    EventDocument.Keys.PersistenceId,
                                    EventDocument.Keys.SequenceNumber
                                }
                            }
                        }));

                _table = Table.LoadTable(_client, _settings.TableName);

                return Events.Initialized.Instance;
            }
            catch (Exception e)
            {
                _log.Error(e, "Failed to initialize table");

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
                    _log.Error(failure.Cause, "Error during journal initialization");
                    Context.Stop(Self);
                    return true;

                //TODO: Remove once the obsolete Failure is removed
                case Failure failure:
                    _log.Error(failure.Exception, "Error during journal initialization");
                    Context.Stop(Self);
                    return true;

                default:
                    Stash?.Stash();
                    return true;
            }
        }

        protected override bool ReceivePluginInternal(object message)
        {
            switch (message)
            {
                case ReplayTaggedMessages replay:
                    ReplayTaggedMessagesAsync(replay)
                        .PipeTo(replay.ReplyTo, success: h => replay.IsCatchup ? new TagCatchupFinished(h) : new RecoverySuccess(h), failure: e => new ReplayMessagesFailure(e));
                    break;
                case SubscribeAllPersistenceIds:
                    AddAllPersistenceIdSubscriber(Sender).PipeTo(Sender);
                    Context.Watch(Sender);
                    break;
                case SubscribeTag subscribe:
                    AddTagSubscriber(Sender, subscribe.Tag);
                    Context.Watch(Sender);
                    break;
                case Terminated terminated:
                    RemoveSubscriber(terminated.ActorRef);
                    break;
                default:
                    return false;
            }

            return true;
        }

        private async Task<long> ReplayTaggedMessagesAsync(ReplayTaggedMessages replay)
        {
            if (replay.FromOffset >= replay.ToOffset)
                return 0;

            var filter = new QueryFilter();
            filter.AddCondition(EventDocument.Keys.Tag, QueryOperator.Equal, replay.Tag);
            filter.AddCondition(EventDocument.Keys.Timestamp, QueryOperator.Between, replay.FromOffset + 1, replay.ToOffset);

            var search = _table!.Query(new QueryOperationConfig
            {
                Filter = filter,
                IndexName = "ByTag",
                Select = SelectValues.AllProjectedAttributes
            });

            var maxOrdering = 0L;
            var replayedItems = 0L;

            while (!search.IsDone)
            {
                var results = (await search.GetNextSetAsync())
                    .Select(x => new EventDocument(x))
                    .ToImmutableList();

                var batchGet = _table.CreateBatchGet();

                foreach (var result in results)
                {
                    batchGet.AddKey(EventDocument.GetEventGroupKey(result.PersistenceId!), result.SequenceNumber);
                }

                await batchGet.ExecuteAsync();

                foreach (var result in batchGet.Results.Select(x => new EventDocument(x)).OrderBy(x => x.Timestamp))
                {
                    if (replayedItems >= replay.Max)
                        return maxOrdering;

                    _log.Debug("Sending replayed message: persistenceId:{0} - sequenceNr:{1}",
                        result.PersistenceId, result.SequenceNumber);
                    
                    foreach (var adaptedRepresentation in AdaptFromJournal(result.ToPersistent(_actorSystem)))
                    {
                        replay.ReplyTo.Tell(new ReplayedTaggedMessage(
                                adaptedRepresentation,
                                replay.Tag,
                                result.Timestamp),
                            ActorRefs.NoSender);
                    }

                    maxOrdering = Math.Max(maxOrdering, result.Timestamp);

                    replayedItems++;
                }
            }

            return maxOrdering;
        }

        private void AddTagSubscriber(IActorRef subscriber, string tag)
        {
            if (!_tagSubscribers.TryGetValue(tag, out var subscriptions))
            {
                subscriptions = new HashSet<IActorRef>();
                _tagSubscribers.Add(tag, subscriptions);
            }

            subscriptions.Add(subscriber);
        }

        private void RemoveSubscriber(IActorRef subscriber)
        {
            var tagSubscriptions = _tagSubscribers.Values.Where(x => x.Contains(subscriber));

            foreach (var subscription in tagSubscriptions)
                subscription.Remove(subscriber);

            _allPersistenceIdSubscribers.Remove(subscriber);
        }

        private async Task AddAllPersistenceIdSubscriber(IActorRef subscriber)
        {
            lock (_allPersistenceIdSubscribers)
            {
                _allPersistenceIdSubscribers.Add(subscriber);
            }

            var filter = new QueryFilter(EventDocument.Keys.DocumentType, QueryOperator.Equal, EventDocument.DocumentTypes.HighestSequenceNumber);

            var search = _table!.Query(new QueryOperationConfig
            {
                Filter = filter,
                IndexName = "ByDocumentType",
                Select = SelectValues.AllProjectedAttributes
            });

            while (!search.IsDone)
            {
                var persistenceIds = (await search.GetNextSetAsync())
                    .Select(x => new EventDocument(x))
                    .Select(x => x.PersistenceId ?? "")
                    .Where(x => !string.IsNullOrEmpty(x))
                    .ToImmutableList();

                subscriber.Tell(new CurrentPersistenceIdsChunk(persistenceIds, search.IsDone));
            }
        }

        private void NotifyTagChange(string tag)
        {
            if (!_tagSubscribers.TryGetValue(tag, out var subscribers))
                return;

            var changed = new TaggedEventAppended(tag);

            foreach (var subscriber in subscribers)
                subscriber.Tell(changed);
        }

        private void NotifyNewPersistenceIdAdded(string persistenceId)
        {
            var added = new PersistenceIdAdded(persistenceId);

            foreach (var subscriber in _allPersistenceIdSubscribers)
                subscriber.Tell(added);
        }
    }
}