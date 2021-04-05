using Akka.Actor;
using Akka.Event;
using Akka.Persistence.DynamoDb.Query.QueryApi;
using Akka.Persistence.Query;
using Akka.Streams.Actors;

namespace Akka.Persistence.DynamoDb.Query.Publishers
{
    internal abstract class AbstractEventsByPersistenceIdPublisher : ActorPublisher<EventEnvelope>
    {
        private ILoggingAdapter? _log;

        protected readonly DeliveryBuffer<EventEnvelope> Buffer;
        protected readonly IActorRef JournalRef;
        protected long CurrentSequenceNr;

        protected AbstractEventsByPersistenceIdPublisher(
            string persistenceId,
            long fromSequenceNr,
            long toSequenceNr,
            int maxBufferSize,
            string writeJournalPluginId)
        {
            PersistenceId = persistenceId;
            CurrentSequenceNr = FromSequenceNr = fromSequenceNr;
            ToSequenceNr = toSequenceNr;
            MaxBufferSize = maxBufferSize;
            Buffer = new DeliveryBuffer<EventEnvelope>(OnNext);

            JournalRef = Persistence.Instance.Apply(Context.System).JournalFor(writeJournalPluginId);
        }

        protected ILoggingAdapter Log => _log ??= Context.GetLogger();
        protected string PersistenceId { get; }
        protected long FromSequenceNr { get; }
        protected long ToSequenceNr { get; set; }
        protected int MaxBufferSize { get; }

        protected bool IsTimeForReplay => (Buffer.IsEmpty || Buffer.Length <= MaxBufferSize / 2) && (CurrentSequenceNr <= ToSequenceNr);

        protected abstract void ReceiveInitialRequest();
        protected abstract void ReceiveIdleRequest();
        protected abstract void ReceiveRecoverySuccess(long highestSequenceNr);

        protected override bool Receive(object message)
        {
            return Init(message);
        }

        protected bool Init(object message)
        {
            return message.Match()
                .With<EventsByPersistenceIdPublisher.Continue>(() => { })
                .With<Request>(_ => ReceiveInitialRequest())
                .With<Cancel>(_ => Context.Stop(Self))
                .WasHandled;
        }

        protected bool Idle(object message)
        {
            return message.Match()
                .With<EventsByPersistenceIdPublisher.Continue>(() => {
                    if (IsTimeForReplay) Replay();
                })
                .With<EventAppended>(() => {
                    if (IsTimeForReplay) Replay();
                })
                .With<Request>(_ => ReceiveIdleRequest())
                .With<Cancel>(_ => Context.Stop(Self))
                .WasHandled;
        }

        protected void Replay()
        {
            var limit = MaxBufferSize - Buffer.Length;
            Log.Debug("request replay for persistenceId [{0}] from [{1}] to [{2}] limit [{3}]", PersistenceId, CurrentSequenceNr, ToSequenceNr, limit);
            JournalRef.Tell(new ReplayMessages(CurrentSequenceNr, ToSequenceNr, limit, PersistenceId, Self));
            Context.Become(Replaying());
        }

        protected Receive Replaying()
        {
            return message => message.Match()
                .With<ReplayedMessage>(replayed => {
                    var seqNr = replayed.Persistent.SequenceNr;
                    Buffer.Add(new EventEnvelope(
                        offset: new Sequence(seqNr),
                        persistenceId: PersistenceId,
                        sequenceNr: seqNr,
                        @event: replayed.Persistent.Payload,
                        timestamp: replayed.Persistent.Timestamp));
                    CurrentSequenceNr = seqNr + 1;
                    Buffer.DeliverBuffer(TotalDemand);
                })
                .With<RecoverySuccess>(success => {
                    Log.Debug("replay completed for persistenceId [{0}], currSeqNo [{1}]", PersistenceId, CurrentSequenceNr);
                    ReceiveRecoverySuccess(success.HighestSequenceNr);
                })
                .With<ReplayMessagesFailure>(failure => {
                    Log.Debug("replay failed for persistenceId [{0}], due to [{1}]", PersistenceId, failure.Cause.Message);
                    Buffer.DeliverBuffer(TotalDemand);
                    OnErrorThenStop(failure.Cause);
                })
                .With<Request>(_ => Buffer.DeliverBuffer(TotalDemand))
                .With<EventsByPersistenceIdPublisher.Continue>(() => { })
                .With<EventAppended>(() => { })
                .With<Cancel>(_ => Context.Stop(Self))
                .WasHandled;
        }
    }
}