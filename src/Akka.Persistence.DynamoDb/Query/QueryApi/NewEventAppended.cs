using System;
using Akka.Event;

namespace Akka.Persistence.DynamoDb.Query.QueryApi
{
    [Serializable]
    public record NewEventAppended : IDeadLetterSuppression
    {
        public static NewEventAppended Instance = new();

        private NewEventAppended() { }
    }
}