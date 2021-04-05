namespace Akka.Persistence.DynamoDb.Query.QueryApi
{
    public record EventReplaySuccess(long HighestSequenceNr)
    {
        public virtual bool Equals(EventReplaySuccess other)
        {
            if (other is null) return false;
            if (ReferenceEquals(this, other)) return true;
        
            return Equals(HighestSequenceNr, other.HighestSequenceNr);
        }

        public override int GetHashCode() => HighestSequenceNr.GetHashCode();

        public override string ToString() => $"EventReplaySuccess<highestSequenceNr: {HighestSequenceNr}>";
    }
}