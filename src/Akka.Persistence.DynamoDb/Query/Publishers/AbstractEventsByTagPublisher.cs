using Akka.Actor;
using Akka.Event;
using Akka.Persistence.DynamoDb.Query.QueryApi;
using Akka.Persistence.Query;
using Akka.Streams.Actors;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Linq;

namespace Akka.Persistence.DynamoDb.Query.Publishers
{
    internal abstract class AbstractEventsByTagPublisher : ActorPublisher<EventEnvelope>
    {
        private ILoggingAdapter? _log;

        protected readonly DeliveryBuffer<EventEnvelope> Buffer;
        protected readonly IActorRef JournalRef;
        private readonly IList<long> _replayed = new List<long>();

        private long _maxAssuredOffset;
        protected long CurrentOffset;

        protected AbstractEventsByTagPublisher(string tag, long fromOffset, int maxBufferSize, string writeJournalPluginId)
        {
            Tag = tag;
            CurrentOffset = _maxAssuredOffset = FromOffset = fromOffset;
            MaxBufferSize = maxBufferSize;
            Buffer = new DeliveryBuffer<EventEnvelope>(OnNext);
            JournalRef = Persistence.Instance.Apply(Context.System).JournalFor(writeJournalPluginId);
        }

        protected ILoggingAdapter Log => _log ??= Context.GetLogger();
        protected string Tag { get; }
        protected long FromOffset { get; }
        protected abstract long ToOffset { get; }
        private int MaxBufferSize { get; }

        private bool IsTimeForReplay => (Buffer.IsEmpty || Buffer.Length <= MaxBufferSize / 2) && (CurrentOffset <= ToOffset);

        protected abstract void ReceiveInitialRequest();
        protected abstract void ReceiveIdleRequest();
        protected abstract void ReceiveRecoverySuccess(long highestSequenceNr);

        protected override bool Receive(object message)
        {
            switch (message)
            {
                case Request:
                    ReceiveInitialRequest();
                    return true;

                case TagCatchupFinished giveUp:
                    GiveUpOnMissingItems(giveUp.HighestSequenceNr);
                    return true;

                case EventsByTagPublisher.Continue:
                    return true;

                case Cancel:
                    Context.Stop(Self);
                    return true;

                default:
                    return false;
            }
        }

        protected bool Idle(object message)
        {
            switch (message)
            {
                case EventsByTagPublisher.Continue:
                case TaggedEventAppended:
                    if (IsTimeForReplay) Replay();
                    return true;

                case TagCatchupFinished giveUp:
                    GiveUpOnMissingItems(giveUp.HighestSequenceNr);
                    return true;

                case Request:
                    ReceiveIdleRequest();
                    return true;

                case Cancel:
                    Context.Stop(Self);
                    return true;

                default:
                    return false;
            }
        }

        protected void Replay()
        {
            var limit = MaxBufferSize - Buffer.Length;
            Log.Debug("request replay for tag [{0}] from [{1}] to [{2}] limit [{3}]", Tag, CurrentOffset, ToOffset, limit);

            if (_maxAssuredOffset < CurrentOffset)
                JournalRef.Tell(new ReplayTaggedMessages(_maxAssuredOffset, CurrentOffset, int.MaxValue, Tag, Self, true));

            JournalRef.Tell(new ReplayTaggedMessages(CurrentOffset, ToOffset, limit, Tag, Self, false));

            Context.Become(Replaying());
        }

        private void GiveUpOnMissingItems(long until)
        {
            if (until <= _maxAssuredOffset) return;

            _maxAssuredOffset = until;

            var oldReplayed = _replayed.Where(x => x < until).ToImmutableList();

            foreach (var item in oldReplayed)
                _replayed.Remove(item);
        }

        private Receive Replaying() => message =>
        {
            switch (message)
            {
                case ReplayedTaggedMessage replayed:

                    if (_replayed.Contains(replayed.Offset))
                    {
                        return true;
                    }

                    Buffer.Add(new EventEnvelope(
                        offset: new Sequence(replayed.Offset),
                        persistenceId: replayed.Persistent.PersistenceId,
                        sequenceNr: replayed.Persistent.SequenceNr,
                        timestamp: replayed.Persistent.Timestamp,
                        @event: replayed.Persistent.Payload));

                    Buffer.DeliverBuffer(TotalDemand);

                    if (replayed.Offset > CurrentOffset)
                    {
                        CurrentOffset = replayed.Offset;
                    }

                    if (replayed.Offset > _maxAssuredOffset)
                    {
                        _replayed.Add(replayed.Offset);
                    }

                    return true;

                case TagCatchupFinished giveUp:
                    GiveUpOnMissingItems(giveUp.HighestSequenceNr);
                    return true;

                case RecoverySuccess success:
                    Log.Debug("replay completed for tag [{0}], currOffset [{1}]", Tag, CurrentOffset);
                    ReceiveRecoverySuccess(success.HighestSequenceNr);
                    return true;

                case ReplayMessagesFailure failure:
                    Log.Debug("replay failed for tag [{0}], due to [{1}]", Tag, failure.Cause.Message);
                    Buffer.DeliverBuffer(TotalDemand);
                    OnErrorThenStop(failure.Cause);
                    return true;

                case Request:
                    Buffer.DeliverBuffer(TotalDemand);
                    
                    if (Buffer.Length < TotalDemand)
                        Replay();
                    
                    return true;

                case EventsByTagPublisher.Continue:
                case TaggedEventAppended:
                    return true;

                case Cancel:
                    Context.Stop(Self);
                    return true;

                default:
                    return false;
            }
        };
    }
}