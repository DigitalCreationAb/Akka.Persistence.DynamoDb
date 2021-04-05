using System;
using Akka.Actor;
using Akka.Event;

namespace Akka.Persistence.DynamoDb.Query.QueryApi
{
    [Serializable]
    public record ReplayedEvent(IPersistentRepresentation Persistent, long Offset) 
        : INoSerializationVerificationNeeded, IDeadLetterSuppression;
}