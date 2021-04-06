using System;
using System.Net;
using System.Net.Sockets;
using Akka.Configuration;

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
        
        public static Config DynamoDbConfig(LocalstackDynamoDbFixture fixture)
        {
            var testId = Guid.NewGuid().ToString().Replace("-", "");
            
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
                auto-initialize = true
                aws-access-key = ""access-key""
                aws-secret-key = ""secret-key""

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
                aws-access-key = ""access-key""
                aws-secret-key = ""secret-key""
            }
        }
    }
}");
        }

    }
}