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

        protected override bool Receive(object message) => message.Match()
            .With<Request>(_ => {
                _journalRef.Tell(SubscribeAllPersistenceIds.Instance);
                Become(Active);
            })
            .With<Cancel>(_ => Context.Stop(Self))
            .WasHandled;

        private bool Active(object message) => message.Match()
            .With<CurrentPersistenceIdsChunk>(current => {
                _buffer.AddRange(current.PersistenceIds);
                _buffer.DeliverBuffer(TotalDemand);

                _replayFinished = current.LastChunk;

                if (!_liveQuery && _replayFinished && _buffer.IsEmpty)
                    OnCompleteThenStop();
            })
            .With<PersistenceIdAdded>(added =>
            {
                if (!_liveQuery) 
                    return;
                
                _buffer.Add(added.PersistenceId);
                _buffer.DeliverBuffer(TotalDemand);
            })
            .With<Request>(_ => {
                _buffer.DeliverBuffer(TotalDemand);
                
                if (!_liveQuery && _replayFinished && _buffer.IsEmpty)
                    OnCompleteThenStop();
            })
            .With<Cancel>(_ => Context.Stop(Self))
            .WasHandled;
    }
}