namespace Akka.Persistence.DynamoDb.Query.QueryApi
{
    public record TagCatchupFinished(long HighestSequenceNr);
}