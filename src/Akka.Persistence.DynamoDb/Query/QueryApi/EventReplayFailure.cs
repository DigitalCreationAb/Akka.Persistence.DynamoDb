using System;

namespace Akka.Persistence.DynamoDb.Query.QueryApi
{
    public record EventReplayFailure(Exception Cause)
    {
        public virtual bool Equals(EventReplayFailure? other)
        {
            if (other is null) return false;
            if (ReferenceEquals(this, other)) return true;

            return Equals(Cause, other.Cause);
        }

        public override int GetHashCode() => Cause.GetHashCode();

        public override string ToString() => $"EventReplayFailure<cause: {Cause.Message}>";
    }
}