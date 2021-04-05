using System.Collections.Immutable;
using Akka.Event;

namespace Akka.Persistence.DynamoDb.Query.QueryApi
{
    public record CurrentPersistenceIdsChunk(IImmutableList<string> PersistenceIds, bool LastChunk) 
        : IDeadLetterSuppression;
}