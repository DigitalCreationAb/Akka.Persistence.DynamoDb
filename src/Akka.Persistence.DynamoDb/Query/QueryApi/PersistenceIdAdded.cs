using Akka.Event;

namespace Akka.Persistence.DynamoDb.Query.QueryApi
{
    public record PersistenceIdAdded(string PersistenceId) : IDeadLetterSuppression;
}