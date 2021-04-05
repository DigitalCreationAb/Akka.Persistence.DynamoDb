using System;
using Amazon.DynamoDBv2;

namespace Akka.Persistence.DynamoDb.Test
{
    public class LocalstackDynamoDbFixture : IDynamoDbFixture
    {
        private readonly IDisposable _localstackInstance;
            
        public LocalstackDynamoDbFixture()
        {
            var mainPort = DynamoDbStorageConfigHelper.GetRandomUnusedPort();
            var servicePort = DynamoDbStorageConfigHelper.GetRandomUnusedPort();
                
            _localstackInstance = Docker.StartDynamoDbLocalstackContainer(mainPort, servicePort);

            AwsServiceUrl = $"http://localhost:{servicePort}";

            var client = new AmazonDynamoDBClient(new AmazonDynamoDBConfig
            {
                ServiceURL = AwsServiceUrl
            });
        }

        public string AwsServiceUrl { get; }
            
        public void Dispose()
        {
            _localstackInstance?.Dispose();
        }
    }
}