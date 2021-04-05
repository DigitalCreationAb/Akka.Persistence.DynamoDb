using System.Collections.Generic;
using System.Collections.Immutable;
using System.Linq;
using System.Threading.Tasks;
using Amazon.DynamoDBv2;
using Amazon.DynamoDBv2.Model;

namespace Akka.Persistence.DynamoDb
{
    public static class AmazonDynamoDBClientExtensions
    {
        public static async Task EnsureTableExistsWithDefinition(
            this IAmazonDynamoDB client,
            string tableName,
            IImmutableList<AttributeDefinition> attributeDefinitions,
            IImmutableList<KeySchemaElement> keySchema,
            IImmutableList<GlobalSecondaryIndex> globalIndices)
        {
            var exists = await client.TableExists(tableName);

            if (exists)
            {
                var currentTable = await client.DescribeTableAsync(tableName);

                var newGlobalIndices = globalIndices
                    .Where(x => currentTable.Table.GlobalSecondaryIndexes.All(y => y.IndexName != x.IndexName))
                    .ToList();

                var removedGlobalIndices = currentTable.Table.GlobalSecondaryIndexes
                    .Where(x => globalIndices.All(y => x.IndexName != y.IndexName))
                    .ToList();

                var indexUpdates = new List<GlobalSecondaryIndexUpdate>();

                indexUpdates.AddRange(newGlobalIndices.Select(x => new GlobalSecondaryIndexUpdate
                {
                    Create = new CreateGlobalSecondaryIndexAction
                    {
                        Projection = x.Projection,
                        IndexName = x.IndexName,
                        KeySchema = x.KeySchema,
                        ProvisionedThroughput = x.ProvisionedThroughput
                    }
                }));

                indexUpdates.AddRange(removedGlobalIndices.Select(x => new GlobalSecondaryIndexUpdate
                {
                    Delete = new DeleteGlobalSecondaryIndexAction
                    {
                        IndexName = x.IndexName
                    }
                }));
                
                await client.UpdateTableAsync(new UpdateTableRequest
                {
                    TableName = tableName,
                    AttributeDefinitions = attributeDefinitions.ToList(),
                    BillingMode = BillingMode.PAY_PER_REQUEST
                });
                
                foreach (var indexUpdate in indexUpdates)
                {
                    await client.UpdateTableAsync(new UpdateTableRequest
                    {
                        TableName = tableName,
                        AttributeDefinitions = attributeDefinitions.ToList(),
                        BillingMode = BillingMode.PAY_PER_REQUEST,
                        GlobalSecondaryIndexUpdates = new List<GlobalSecondaryIndexUpdate>
                        {
                            indexUpdate
                        }
                    });
                }
            }
            else
            {
                await client.CreateTableAsync(new CreateTableRequest
                {
                    AttributeDefinitions = attributeDefinitions.ToList(),
                    BillingMode = BillingMode.PAY_PER_REQUEST,
                    KeySchema = keySchema.ToList(),
                    TableName = tableName,
                    GlobalSecondaryIndexes = globalIndices.ToList()
                });
            }
        }
        
        public static async Task<bool> TableExists(this IAmazonDynamoDB client, string tableName)
        {
            try
            {
                await client.DescribeTableAsync(tableName);

                return true;
            }
            catch (ResourceNotFoundException)
            {
                return false;
            }
        }
    }
}