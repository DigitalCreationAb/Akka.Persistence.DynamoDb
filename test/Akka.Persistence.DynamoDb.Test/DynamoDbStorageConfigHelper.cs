using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Net;
using System.Net.Sockets;
using Akka.Configuration;
using Akka.Persistence.DynamoDb.Journal;
using Amazon.DynamoDBv2;
using Amazon.DynamoDBv2.Model;

namespace Akka.Persistence.DynamoDb.Test
{
    public static class DynamoDbStorageConfigHelper
    {
        public static int GetRandomUnusedPort()
        {
            var listener = new TcpListener(IPAddress.Any, 0);
            listener.Start();
            var port = ((IPEndPoint)listener.LocalEndpoint).Port;
            listener.Stop();
            return port;
        }

        public static Config DynamoDbConfig(DynamoDbDatabaseFixture fixture)
        {
            var testId = Guid.NewGuid().ToString().Replace("-", "");

            fixture.DdbClient.EnsureTableExistsWithDefinition(
                $"J{testId}",
                new List<AttributeDefinition>
                {
                    new(EventDocument.Keys.GroupKey, ScalarAttributeType.S),
                    new(EventDocument.Keys.SequenceNumber, ScalarAttributeType.N),
                    new(EventDocument.Keys.DocumentType, ScalarAttributeType.S),
                    new(EventDocument.Keys.PersistenceId, ScalarAttributeType.S),
                    new(EventDocument.Keys.Tag, ScalarAttributeType.S),
                    new(EventDocument.Keys.Timestamp, ScalarAttributeType.N)
                }.ToImmutableList(),
                new List<KeySchemaElement>
                {
                    new(EventDocument.Keys.GroupKey, KeyType.HASH),
                    new(EventDocument.Keys.SequenceNumber, KeyType.RANGE)
                }.ToImmutableList(),
                ImmutableList.Create(new GlobalSecondaryIndex
                    {
                        IndexName = "ByDocumentType",
                        KeySchema = new List<KeySchemaElement>
                        {
                            new(EventDocument.Keys.DocumentType, KeyType.HASH),
                            new(EventDocument.Keys.PersistenceId, KeyType.RANGE)
                        },
                        Projection = new Projection
                        {
                            ProjectionType = ProjectionType.KEYS_ONLY
                        }
                    },
                    new GlobalSecondaryIndex
                    {
                        IndexName = "ByTag",
                        KeySchema = new List<KeySchemaElement>
                        {
                            new(EventDocument.Keys.Tag, KeyType.HASH),
                            new(EventDocument.Keys.Timestamp, KeyType.RANGE)
                        },
                        Projection = new Projection
                        {
                            ProjectionType = ProjectionType.INCLUDE,
                            NonKeyAttributes = new List<string>
                            {
                                EventDocument.Keys.PersistenceId,
                                EventDocument.Keys.SequenceNumber
                            }
                        }
                    })).Wait();

            return ConfigurationFactory.ParseString(
                @"
akka {
    loglevel = DEBUG
    log-config-on-start = off
    test.single-expect-default = 30s

    persistence {
        publish-plugin-commands = on

        journal {
            plugin = ""akka.persistence.journal.dynamodb""

            dynamodb {
                class = ""Akka.Persistence.DynamoDb.Journal.DynamoDbJournal, Akka.Persistence.DynamoDb""
                table-name = ""J" + testId + @"""
                aws-service-url = """ + fixture.AwsServiceUrl + @"""
                auto-initialize = false
                aws-access-key = ""accesskey""
                aws-secret-key = ""secretkey""

                event-adapters {
                    color-tagger = ""Akka.Persistence.TCK.Query.ColorFruitTagger, Akka.Persistence.TCK""
                }
                event-adapter-bindings = {
                    ""System.String"" = color-tagger
                }
            }
        }

        query {
            journal {
                dynamodb {
                    class = ""Akka.Persistence.DynamoDb.Query.DynamoDbReadJournalProvider, Akka.Persistence.DynamoDb""
                    write-plugin = ""akka.persistence.journal.dynamodb""
                    refresh-interval = 1s
		            max-buffer-size = 150
                }
            }
        }

        snapshot-store {
            plugin = ""akka.persistence.snapshot-store.dynamodb""
            
            dynamodb {
                class = ""Akka.Persistence.DynamoDb.Snapshot.DynamoDbSnapshotStore, Akka.Persistence.DynamoDb""
                table-name = ""S" + testId + @"""
                aws-service-url = """ + fixture.AwsServiceUrl + @"""
                auto-initialize = true
                aws-access-key = ""accesskey""
                aws-secret-key = ""secretkey""
            }
        }
    }
}");
        }
    }
}