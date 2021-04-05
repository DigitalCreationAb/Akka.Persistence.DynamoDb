using System;

namespace Akka.Persistence.DynamoDb.Test
{
    public interface IDynamoDbFixture : IDisposable
    {
        string AwsServiceUrl { get; }
    }
}