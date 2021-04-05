using System;
using Akka.Actor;

namespace Akka.Persistence.DynamoDb.Query.QueryApi
{
    [Serializable]
    public record ReplayAllEvents(long FromOffset, long ToOffset, long Max, IActorRef ReplyTo) : IJournalRequest;
}