namespace Akka.Persistence.DynamoDb
{
    public interface IDynamoDbSettings
    {
        string TableName { get; }
        string AwsRegion { get; }
        string AwsAccessKey { get; }
        string AwsSecretKey { get; }
        string AwsServiceUrl { get; }
        bool AutoInitialize { get; }
    }
}