using System;
using Akka.Event;

namespace Akka.Persistence.DynamoDb.Query.QueryApi
{
    [Serializable]
    public record EventAppended(string PersistenceId) : IDeadLetterSuppression;
}