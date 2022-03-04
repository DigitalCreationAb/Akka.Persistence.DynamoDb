using System;
using Akka.Actor;

namespace Akka.Persistence.DynamoDb.Query.QueryApi
{
    [Serializable]
    public record ReplayTaggedMessages(
            long FromOffset,
            long ToOffset,
            long Max,
            string Tag,
            IActorRef ReplyTo,
            bool IsCatchup) 
        : IJournalRequest;
}