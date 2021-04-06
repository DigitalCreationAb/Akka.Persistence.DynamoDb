using System;
using Amazon.DynamoDBv2;

namespace Akka.Persistence.DynamoDb.Test
{
    public class LocalstackDynamoDbFixture : IDynamoDbFixture
    {
        private readonly IDisposable _localstackInstance;

        protected LocalstackDynamoDbFixture()
        {
            var dynamodbUrl = Environment.GetEnvironmentVariable("AWS_DYNAMODB_URL");

            if (!string.IsNullOrEmpty(dynamodbUrl))
            {
                AwsServiceUrl = dynamodbUrl;
                
                return;
            }
        
            var mainPort = DynamoDbStorageConfigHelper.GetRandomUnusedPort();
            var servicePort = DynamoDbStorageConfigHelper.GetRandomUnusedPort();
                
            _localstackInstance = Docker.StartDynamoDbLocalstackContainer(mainPort, servicePort);

            AwsServiceUrl = $"http://127.0.0.1:{servicePort}";
        }

        public string AwsServiceUrl { get; }
            
        public void Dispose()
        {
            _localstackInstance?.Dispose();
        }
    }
}