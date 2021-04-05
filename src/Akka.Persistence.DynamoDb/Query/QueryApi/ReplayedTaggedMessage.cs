using System;
using Akka.Actor;
using Akka.Event;

namespace Akka.Persistence.DynamoDb.Query.QueryApi
{
    [Serializable]
    public record ReplayedTaggedMessage(IPersistentRepresentation Persistent, string Tag, long Offset)
        : INoSerializationVerificationNeeded, IDeadLetterSuppression;
}