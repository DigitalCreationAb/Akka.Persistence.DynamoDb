using Akka.Actor;

namespace Akka.Persistence.DynamoDb.Query.Publishers
{
    internal sealed class CurrentEventsByTagPublisher : AbstractEventsByTagPublisher
    {
        private long _toOffset;

        public CurrentEventsByTagPublisher(
            string tag, 
            long fromOffset, 
            long toOffset, 
            int maxBufferSize, 
            string writeJournalPluginId)
            : base(tag, fromOffset, maxBufferSize, writeJournalPluginId)
        {
            _toOffset = toOffset;
        }

        protected override long ToOffset => _toOffset;

        protected override void ReceiveIdleRequest()
        {
            Buffer.DeliverBuffer(TotalDemand);

            if (Buffer.IsEmpty && CurrentOffset >= ToOffset)
                OnCompleteThenStop();
            else
                Self.Tell(EventsByTagPublisher.Continue.Instance);
        }

        protected override void ReceiveInitialRequest()
        {
            Replay();
        }

        protected override void ReceiveRecoverySuccess(long highestSequenceNr)
        {
            Buffer.DeliverBuffer(TotalDemand);

            if (highestSequenceNr < ToOffset)
                _toOffset = highestSequenceNr;

            if (Buffer.IsEmpty && (CurrentOffset > ToOffset || CurrentOffset == FromOffset))
                OnCompleteThenStop();
            else
                Self.Tell(EventsByTagPublisher.Continue.Instance);

            Context.Become(Idle);
        }
    }
}