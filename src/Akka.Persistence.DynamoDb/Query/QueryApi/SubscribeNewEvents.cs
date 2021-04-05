using System;

namespace Akka.Persistence.DynamoDb.Query.QueryApi
{
    [Serializable]
    public record SubscribeNewEvents : ISubscriptionCommand
    {
        public static SubscribeNewEvents Instance = new();

        private SubscribeNewEvents() { }
    }
}