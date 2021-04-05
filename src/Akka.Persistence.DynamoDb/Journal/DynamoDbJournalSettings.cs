using Akka.Configuration;

namespace Akka.Persistence.DynamoDb.Journal
{
    public record DynamoDbJournalSettings(
        string TableName,
        string AwsRegion,
        string AwsAccessKey,
        string AwsSecretKey,
        string AwsServiceUrl,
        bool AutoInitialize) : IDynamoDbSettings
    {
        public static DynamoDbJournalSettings Create(Config config)
        {
            var tableName = config.GetString("table-name");
            var awsRegion = config.GetString("aws-region");
            var awsAccessKey = config.GetString("aws-access-key");
            var awsSecretKey = config.GetString("aws-secret-key");
            var awsServiceUrl = config.GetString("aws-service-url");
            var autoInitialize = config.GetBoolean("auto-initialize");
            
            return new DynamoDbJournalSettings(
                tableName,
                awsRegion,
                awsAccessKey,
                awsSecretKey,
                awsServiceUrl,
                autoInitialize);
        }
    }
}