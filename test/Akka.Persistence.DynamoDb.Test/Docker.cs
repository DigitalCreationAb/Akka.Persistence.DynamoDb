using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Diagnostics;
using System.Linq;
using System.Net.Http;
using System.Net.Http.Json;
using System.Threading;

namespace Akka.Persistence.DynamoDb.Test
{
    public static class Docker
    {
        public static IDisposable StartDynamoDbLocalstackContainer(int mainPort, int servicesPort)
        {
            bool WaitForStart(TimeSpan timeout)
            {
                Console.WriteLine($"Waiting for localstack to start. (main port: {mainPort}, services port: {servicesPort})");

                var timer = Stopwatch.StartNew();
                var httpClient = new HttpClient();

                while (timer.Elapsed < timeout)
                {
                    try
                    {
                        var response = httpClient.GetAsync($"http://127.0.0.1:{mainPort}/health").Result;

                        if (response.IsSuccessStatusCode)
                        {
                            var responseData = response.Content.ReadFromJsonAsync<HealthResponse>().Result ??
                                               new HealthResponse
                                               {
                                                   Services = ImmutableDictionary<string, string>.Empty
                                               };
                            
                            Console.WriteLine($"Polled localstack with result: {response.Content.ReadAsStringAsync().Result}");

                            if (responseData.Services.Any()
                                && responseData.Services.ContainsKey("dynamodb") &&
                                responseData.Services["dynamodb"] == "running")
                            {
                                Console.WriteLine($"Running services: {string.Join(", ", responseData.Services.Keys)}");

                                return true;
                            }
                        }

                        Thread.Sleep(TimeSpan.FromSeconds(5));
                    }
                    catch (Exception exception)
                    {
                        Console.WriteLine($"Failed polling localstack: {exception.Message}");
                        
                        Thread.Sleep(TimeSpan.FromSeconds(5));
                    }
                }

                return false;
            }

            var containerName = Guid.NewGuid().ToString();

            var runningContainer = ContainerFromImage("localstack/localstack:latest", containerName)
                .Detached()
                .WithDockerSocket()
                .Port(mainPort, 8080)
                .Port(servicesPort, 4566)
                .EnvironmentVariable("SERVICES", "dynamodb")
                .EnvironmentVariable("DEBUG", "1")
                .Run();

            if (WaitForStart(TimeSpan.FromMinutes(2)))
                return runningContainer;

            runningContainer.Dispose();

            throw new Exception("Couldn't start localstack in time");
        }

        private static Container ContainerFromImage(string image, string name)
        {
            return new(name, image, new List<string>().ToImmutableList());
        }

        public class Container
        {
            private readonly string _image;
            private readonly IImmutableList<string> _dockerArguments;

            public Container(string name, string image, IImmutableList<string> dockerArguments)
            {
                _image = image;
                _dockerArguments = dockerArguments ?? new List<string>().ToImmutableList();
                Name = name;
            }

            public string Name { get; }

            public Container Detached()
            {
                return WithArgument("-d");
            }

            public Container Port(int host, int container)
            {
                return WithArgument($"-p {host}:{container}");
            }

            public Container EnvironmentVariable(string name, string value)
            {
                return WithArgument($"-e {name}=\"{value}\"");
            }

            public Container WithDockerSocket()
            {
                return WithArgument("-v \"/var/run/docker.sock:/var/run/docker.sock\"");
            }

            public Container WithArgument(string argument)
            {
                var newArguments = !_dockerArguments.Contains(argument)
                    ? _dockerArguments.Add(argument)
                    : _dockerArguments;

                return new Container(Name, _image, newArguments);
            }

            public IDisposable Run()
            {
                var arguments = string.Join(" ", _dockerArguments);

                if (!string.IsNullOrEmpty(Name))
                    arguments = $"--name {Name} {arguments}";

                var startInfo = new ProcessStartInfo
                {
                    FileName = "docker",
                    Arguments = $"run {arguments} {_image}",
                    WindowStyle = ProcessWindowStyle.Hidden,
                    CreateNoWindow = true,
                    RedirectStandardError = true,
                    RedirectStandardOutput = true
                };

                var process = Process.Start(startInfo);

                if (process != null)
                {
                    process.ErrorDataReceived += (_, args) => Console.WriteLine(args.Data);
                    process.OutputDataReceived += (_, args) => Console.WriteLine(args.Data);

                    process.BeginErrorReadLine();
                    process.BeginOutputReadLine();
                }

                process?.WaitForExit();

                return new StoppableContainer(Name);
            }

            private class StoppableContainer : IDisposable
            {
                private readonly string _name;

                public StoppableContainer(string name)
                {
                    _name = name;
                }

                public void Dispose()
                {
                    var startInfo = new ProcessStartInfo
                    {
                        FileName = "docker",
                        Arguments = $"container rm -f {_name}"
                    };

                    var process = Process.Start(startInfo);

                    process?.WaitForExit();
                }
            }
        }

        private class HealthResponse
        {
            public IImmutableDictionary<string, string> Services { get; set; }
        }
    }
}