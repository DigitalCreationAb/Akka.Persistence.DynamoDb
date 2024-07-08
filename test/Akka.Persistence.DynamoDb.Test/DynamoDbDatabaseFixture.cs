using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Threading.Tasks;
using Amazon.DynamoDBv2;
using Amazon.Runtime;
using Docker.DotNet;
using Docker.DotNet.Models;
using Xunit;

namespace Akka.Persistence.DynamoDb.Test;

public class DynamoDbDatabaseFixture : IAsyncLifetime
{
    private const string DynamoDbImage = "amazon/dynamodb-local";
    private const string DynamoDbImageTag = "latest";

    private readonly string _dynamoDbContainerName = $"ddb-{Guid.NewGuid():N}";

    private DockerClient _dockerClient;
    
    public IAmazonDynamoDB DdbClient { get; private set; }
    
    public string AwsServiceUrl { get; private set; }
    
    public async Task InitializeAsync()
    {
        _dockerClient = new DockerClientConfiguration().CreateClient();

        var images = await _dockerClient.Images.ListImagesAsync(new ImagesListParameters
        {
            Filters = new Dictionary<string, IDictionary<string, bool>>
            {
                {
                    "reference",
                    new Dictionary<string, bool>
                    {
                        { DynamoDbImage, true }
                    }
                }
            }
        });

        if (images.Count == 0)
        {
            await _dockerClient.Images.CreateImageAsync(
                new ImagesCreateParameters { FromImage = DynamoDbImage, Tag = DynamoDbImageTag }, null,
                new Progress<JSONMessage>(message =>
                {
                    Console.WriteLine(!string.IsNullOrEmpty(message.ErrorMessage)
                        ? message.ErrorMessage
                        : $"{message.ID} {message.Status} {message.ProgressMessage}");
                }));
        }

        var mainPort = DynamoDbStorageConfigHelper.GetRandomUnusedPort();

        await _dockerClient.Containers.CreateContainerAsync(
            new CreateContainerParameters
            {
                Image = DynamoDbImage,
                Name = _dynamoDbContainerName,
                Tty = true,
                Cmd = new List<string>
                {
                    "-jar", 
                    "DynamoDBLocal.jar", 
                    "-sharedDb",
                    "-inMemory"
                },
                ExposedPorts = new Dictionary<string, EmptyStruct>
                {
                    { "8000/tcp", new EmptyStruct() }
                },
                HostConfig = new HostConfig
                {
                    PortBindings = new Dictionary<string, IList<PortBinding>>
                    {
                        ["8000/tcp"] = new List<PortBinding>
                        {
                            new()
                            {
                                HostPort = mainPort.ToString()
                            }
                        }
                    }
                }
            });

        await _dockerClient.Containers.StartContainerAsync(
            _dynamoDbContainerName,
            new ContainerStartParameters());

        AwsServiceUrl = $"http://localhost:{mainPort}";

        DdbClient = new AmazonDynamoDBClient(
            new BasicAWSCredentials("accesskey", "secretkey"),
            new AmazonDynamoDBConfig
            {
                ServiceURL = AwsServiceUrl
            });
        
        if (await WaitForStart(TimeSpan.FromMinutes(2)))
            return;

        await DisposeAsync();

        throw new Exception("Couldn't start localstack in time");

        async Task<bool> WaitForStart(TimeSpan timeout)
        {
            var elapsed = Stopwatch.StartNew();

            while (elapsed.Elapsed < timeout)
            {
                try
                {
                    await DdbClient.ListTablesAsync();

                    return true;
                }
                catch (Exception)
                {
                    await Task.Delay(TimeSpan.FromMilliseconds(200));
                }
            }

            return false;
        }
    }

    public async Task DisposeAsync()
    {
        if (_dockerClient != null)
        {
            await _dockerClient.Containers.StopContainerAsync(_dynamoDbContainerName,
                new ContainerStopParameters { WaitBeforeKillSeconds = 0 });
            await _dockerClient.Containers.RemoveContainerAsync(_dynamoDbContainerName,
                new ContainerRemoveParameters { Force = true });
            _dockerClient.Dispose();
        }
    }
}