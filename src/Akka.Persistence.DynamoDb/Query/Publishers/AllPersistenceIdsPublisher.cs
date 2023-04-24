using Akka.Actor;
using Akka.Persistence.DynamoDb.Query.QueryApi;
using Akka.Streams.Actors;

namespace Akka.Persistence.DynamoDb.Query.Publishers
{
    internal sealed class AllPersistenceIdsPublisher : ActorPublisher<string>
    {
        public static Props Props(bool liveQuery, string writeJournalPluginId)
        {
            return Actor.Props.Create(() => new AllPersistenceIdsPublisher(liveQuery, writeJournalPluginId));
        }

        private readonly bool _liveQuery;
        private readonly IActorRef _journalRef;
        private readonly DeliveryBuffer<string> _buffer;

        private bool _replayFinished;

        public AllPersistenceIdsPublisher(bool liveQuery, string writeJournalPluginId)
        {
            _liveQuery = liveQuery;
            _buffer = new DeliveryBuffer<string>(OnNext);
            _journalRef = Persistence.Instance.Apply(Context.System).JournalFor(writeJournalPluginId);
        }

        protected override bool Receive(object message)
        {
            switch (message)
            {
                case Request:
                    _journalRef.Tell(SubscribeAllPersistenceIds.Instance);
                    Become(Active);
                    return true;

                case Cancel:
                    Context.Stop(Self);
                    return true;

                default:
                    return false;
            }
        }

        private bool Active(object message)
        {
            switch (message)
            {
                case CurrentPersistenceIdsChunk current:

                    _buffer.AddRange(current.PersistenceIds);
                    _buffer.DeliverBuffer(TotalDemand);

                    _replayFinished = current.LastChunk;

                    if (!_liveQuery && _replayFinished && _buffer.IsEmpty)
                    {
                        OnCompleteThenStop();
                    }

                    return true;

                case PersistenceIdAdded added:

                    if (!_liveQuery)
                    {
                        return true;
                    }

                    _buffer.Add(added.PersistenceId);
                    _buffer.DeliverBuffer(TotalDemand);

                    return true;

                case Request:

                    _buffer.DeliverBuffer(TotalDemand);

                    if (!_liveQuery && _replayFinished && _buffer.IsEmpty)
                    {
                        OnCompleteThenStop();
                    }

                    return true;

                case Cancel:
                    Context.Stop(Self);
                    return true;

                default:
                    return false;
            }
        }
    }
}