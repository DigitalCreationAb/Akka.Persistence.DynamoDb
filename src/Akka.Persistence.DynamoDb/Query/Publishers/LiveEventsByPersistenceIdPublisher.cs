using System;
using Akka.Actor;

namespace Akka.Persistence.DynamoDb.Query.Publishers
{
    internal sealed class LiveEventsByPersistenceIdPublisher : AbstractEventsByPersistenceIdPublisher
    {
        private readonly ICancelable _tickCancelable;

        public LiveEventsByPersistenceIdPublisher(
            string persistenceId,
            long fromSequenceNr,
            long toSequenceNr,
            int maxBufferSize,
            string writeJournalPluginId,
            TimeSpan refreshInterval)
            : base(persistenceId, fromSequenceNr, toSequenceNr, maxBufferSize, writeJournalPluginId)
        {
            _tickCancelable = Context
                .System
                .Scheduler
                .ScheduleTellRepeatedlyCancelable(refreshInterval, refreshInterval, Self, EventsByPersistenceIdPublisher.Continue.Instance, Self);
        }

        protected override void PostStop()
        {
            _tickCancelable.Cancel();
            base.PostStop();
        }

        protected override void ReceiveInitialRequest()
        {
            Replay();
        }

        protected override void ReceiveIdleRequest()
        {
            Buffer.DeliverBuffer(TotalDemand);
            if (Buffer.IsEmpty && CurrentSequenceNr > ToSequenceNr)
                OnCompleteThenStop();
        }

        protected override void ReceiveRecoverySuccess(long highestSequenceNr)
        {
            Buffer.DeliverBuffer(TotalDemand);
            if (Buffer.IsEmpty && CurrentSequenceNr > ToSequenceNr)
                OnCompleteThenStop();

            Context.Become(Idle);
        }
    }
}