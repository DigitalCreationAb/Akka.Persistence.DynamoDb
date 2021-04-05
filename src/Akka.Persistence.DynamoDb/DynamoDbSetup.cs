using Amazon;
using Amazon.DynamoDBv2;
using Amazon.DynamoDBv2.DocumentModel;
using Amazon.Runtime;

namespace Akka.Persistence.DynamoDb
{
    public class DynamoDbSetup
    {
        public static AmazonDynamoDBClient InitClient(IDynamoDbSettings settings)
        {
            var sessionConfig = new AmazonDynamoDBConfig();
            
            if (!string.IsNullOrEmpty(settings.AwsRegion))
                sessionConfig.RegionEndpoint = RegionEndpoint.GetBySystemName(settings.AwsRegion);

            if (!string.IsNullOrEmpty(settings.AwsServiceUrl))
                sessionConfig.ServiceURL = settings.AwsServiceUrl;

            var credentials = GetCredentials(settings);
            
            return new AmazonDynamoDBClient(credentials, sessionConfig);
        }
        
        private static AWSCredentials GetCredentials(IDynamoDbSettings settings)
        {
            if (!string.IsNullOrEmpty(settings.AwsAccessKey) && !string.IsNullOrEmpty(settings.AwsSecretKey))
            {
                return new BasicAWSCredentials(settings.AwsAccessKey, settings.AwsSecretKey);
            }

            return FallbackCredentialsFactory.GetCredentials();
        }
    }
}