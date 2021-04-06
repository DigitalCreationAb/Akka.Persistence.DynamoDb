using System;
using Akka.Configuration;
using Akka.Persistence.DynamoDb.Query.Publishers;
using Akka.Persistence.Query;
using Akka.Streams.Dsl;

namespace Akka.Persistence.DynamoDb.Query
{
    public class DynamoDbReadJournal : IPersistenceIdsQuery,
        ICurrentPersistenceIdsQuery,
        IEventsByPersistenceIdQuery,
        ICurrentEventsByPersistenceIdQuery,
        IEventsByTagQuery,
        ICurrentEventsByTagQuery
    {
        private readonly TimeSpan _refreshInterval;
        private readonly string _writeJournalPluginId;
        private readonly int _maxBufferSize;

        public DynamoDbReadJournal(Config config)
        {
            _refreshInterval = config.GetTimeSpan("refresh-interval");
            _writeJournalPluginId = config.GetString("write-plugin");
            _maxBufferSize = config.GetInt("max-buffer-size");
        }

        public const string Identifier = "akka.persistence.query.journal.dynamodb";

        public Source<string, NotUsed> PersistenceIds()
        {
            return Source.ActorPublisher<string>(AllPersistenceIdsPublisher.Props(true, _writeJournalPluginId))
                .MapMaterializedValue(_ => NotUsed.Instance)
                .Named("AllPersistenceIds");
        }

        public Source<string, NotUsed> CurrentPersistenceIds()
        {
            return Source.ActorPublisher<string>(AllPersistenceIdsPublisher.Props(false, _writeJournalPluginId))
                .MapMaterializedValue(_ => NotUsed.Instance)
                .Named("CurrentPersistenceIds");
        }
        
        public Source<EventEnvelope, NotUsed> EventsByPersistenceId(
            string persistenceId,
            long fromSequenceNr,
            long toSequenceNr)
        {
            return Source.ActorPublisher<EventEnvelope>(EventsByPersistenceIdPublisher.Props(
                    persistenceId,
                    fromSequenceNr,
                    toSequenceNr,
                    _refreshInterval,
                    _maxBufferSize,
                    _writeJournalPluginId))
                .MapMaterializedValue(_ => NotUsed.Instance)
                .Named("EventsByPersistenceId-" + persistenceId);
        }

        public Source<EventEnvelope, NotUsed> CurrentEventsByPersistenceId(
            string persistenceId,
            long fromSequenceNr,
            long toSequenceNr)
        {
            return Source.ActorPublisher<EventEnvelope>(EventsByPersistenceIdPublisher.Props(
                    persistenceId,
                    fromSequenceNr,
                    toSequenceNr,
                    null,
                    _maxBufferSize,
                    _writeJournalPluginId))
                .MapMaterializedValue(_ => NotUsed.Instance)
                .Named("CurrentEventsByPersistenceId-" + persistenceId);
        }

        public Source<EventEnvelope, NotUsed> EventsByTag(string tag, Offset? offset = null)
        {
            offset ??= new Sequence(0L);

            return offset switch
            {
                Sequence seq => Source
                    .ActorPublisher<EventEnvelope>(EventsByTagPublisher.Props(
                        tag,
                        seq.Value,
                        long.MaxValue,
                        _refreshInterval,
                        _maxBufferSize,
                        _writeJournalPluginId))
                    .MapMaterializedValue(_ => NotUsed.Instance)
                    .Named($"EventsByTag-{tag}"),
                NoOffset _ => EventsByTag(tag, new Sequence(0L)),
                _ => throw new ArgumentException($"{GetType().Name} does not support {offset.GetType().Name} offsets")
            };
        }

        public Source<EventEnvelope, NotUsed> CurrentEventsByTag(string tag, Offset? offset = null)
        {
            offset ??= new Sequence(0L);

            return offset switch
            {
                Sequence seq => Source
                    .ActorPublisher<EventEnvelope>(EventsByTagPublisher.Props(
                        tag,
                        seq.Value,
                        long.MaxValue,
                        null,
                        _maxBufferSize,
                        _writeJournalPluginId))
                    .MapMaterializedValue(_ => NotUsed.Instance)
                    .Named($"CurrentEventsByTag-{tag}"),
                NoOffset _ => CurrentEventsByTag(tag, new Sequence(0L)),
                _ => throw new ArgumentException($"{GetType().Name} does not support {offset.GetType().Name} offsets")
            };
        }
    }
}