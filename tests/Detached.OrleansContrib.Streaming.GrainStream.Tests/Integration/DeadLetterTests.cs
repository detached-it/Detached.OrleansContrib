// -----------------------------------------------------------------------
// <copyright file="DeadLetterTests.cs" company="Detached IT">
//     ©2026 Detached IT. All rights reserved
// </copyright>
// -----------------------------------------------------------------------

using Detached.OrleansContrib.Streaming.GrainStream.Configuration;
using Detached.OrleansContrib.Streaming.GrainStream.Extensions;
using Detached.OrleansContrib.Streaming.GrainStream.Grains;
using Detached.OrleansContrib.Streaming.GrainStream.Models;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Orleans.TestingHost;

namespace Detached.OrleansContrib.Streaming.GrainStream.Tests.Integration;

public class DeadLetterClusterFixture : IAsyncLifetime
{
    public TestCluster? Cluster { get; private set; }
    public const string StreamProviderName = "GrainStreamDeadLetter";

    public async Task InitializeAsync()
    {
        var builder = new TestClusterBuilder();
        builder.AddSiloBuilderConfigurator<SiloConfigurator>();
        builder.AddClientBuilderConfigurator<ClientConfigurator>();
        Cluster = builder.Build();
        await Cluster.DeployAsync();
    }

    public async Task DisposeAsync()
    {
        if (Cluster is not null)
        {
            await Cluster.StopAllSilosAsync();
            await Cluster.DisposeAsync();
        }
    }

    private sealed class SiloConfigurator : ISiloConfigurator
    {
        public void Configure(ISiloBuilder siloBuilder)
        {
            siloBuilder
                .ConfigureLogging(logging => logging.SetMinimumLevel(LogLevel.Debug))
                .AddMemoryGrainStorage("GrainStreamStore")
                .AddMemoryGrainStorage("PubSubStore")
                .UseInMemoryReminderService()
                .UseTransactions() // Essential for the grain
                .AddGrainStream(StreamProviderName, options =>
                {
                    options.NumQueues = 1;
                    options.InFlightTimeoutSeconds = 1; // Short timeout for testing
                    options.ReminderIntervalSeconds = 60; // Don't rely on reminders automatically
                    options.StorageProviderName = "GrainStreamStore";
                })
                .ConfigureServices(services =>
                {
                    services.Configure<GrainStreamOptions>(options =>
                    {
                        options.InFlightTimeoutSeconds = 1;
                        options.ReminderIntervalSeconds = 60;
                    });
                });
        }
    }

    private sealed class ClientConfigurator : IClientBuilderConfigurator
    {
        public void Configure(IConfiguration configuration, IClientBuilder clientBuilder)
        {
            clientBuilder.AddGrainStream(StreamProviderName, options =>
            {
                options.NumQueues = 1;
                options.InFlightTimeoutSeconds = 1;
            });
        }
    }
}

[CollectionDefinition("DeadLetterCollection")]
public class DeadLetterCollection : ICollectionFixture<DeadLetterClusterFixture>
{
}

[Collection("DeadLetterCollection")]
public sealed class DeadLetterTests(DeadLetterClusterFixture fixture)
{
    private readonly TestCluster _cluster = fixture.Cluster ?? throw new InvalidOperationException("Cluster not initialised");

    [Fact]
    public async Task ProcessDeadLetterCheckAsync_RequeuesTimedOutMessages()
    {
        var client = _cluster.Client;
        // Use a specific queue grain to control the test
        // The queue is "GrainStreamDeadLetter_0" usually?

        var queueGrain = client.GetGrain<IStreamQueueGrain>("GrainStreamDeadLetter_0");

        var streamId = StreamId.Create("TestNs", "dead-letter-test");
        var msg = GrainStreamBatchContainer.ToMessage(streamId, "payload");

        // 1. Enqueue
        await queueGrain.EnqueueAsync([msg]);

        // 2. Dequeue (moves to InFlight)
        var dequeued = await queueGrain.DequeueAsync(1);
        Assert.Single(dequeued);

        // 3. Wait for timeout (1s + buffer)
        await Task.Delay(2000);

        // 4. Trigger DeadLetter Check
        await queueGrain.ProcessDeadLetterCheckAsync();

        // 5. Verify it is back in Pending (QueueLength = 1)
        var length = await queueGrain.GetQueueLengthAsync();
        Assert.Equal(1, length);

        // 6. Dequeue again to verify it's the same message
        var redequeued = await queueGrain.DequeueAsync(1);
        Assert.Single(redequeued);
        Assert.Equal(msg.MessageId, redequeued[0].MessageId);
    }
}
