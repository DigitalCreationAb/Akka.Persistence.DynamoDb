using System;

namespace Akka.Persistence.DynamoDb.Query.QueryApi
{
    [Serializable]
    public record SubscribeTag(string Tag) : ISubscriptionCommand;
}