// -----------------------------------------------------------------------
// <copyright file="ClusterFixture.cs" company="Detached IT">
//     ©2026 Detached IT. All rights reserved
// </copyright>
// -----------------------------------------------------------------------

using Detached.OrleansContrib.Streaming.GrainStream.Extensions;
using Microsoft.Extensions.Configuration;
using Orleans.TestingHost;

namespace Detached.OrleansContrib.Streaming.GrainStream.Tests.Integration;

/// <summary>
/// Provides a TestCluster configured with the GrainStream provider for integration tests.
/// Uses in-memory storage, in-memory transactions, and in-memory reminders.
/// </summary>
public sealed class ClusterFixture : IAsyncLifetime
{
    public TestCluster? Cluster { get; private set; }

    public const string StreamProviderName = "GrainStreamTest";

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
                .AddMemoryGrainStorage("GrainStreamStore")
                .AddMemoryGrainStorageAsDefault() // Fallback
                .AddMemoryGrainStorage("PubSubStore")
                .AddMemoryGrainStorage("PubSubStore")
                .UseInMemoryReminderService()
                .UseTransactions()
                .AddGrainStream(StreamProviderName, options =>
                {
                    options.NumQueues = 4;
                    options.MaxBatchSize = 50;
                    options.InFlightTimeoutSeconds = 10;
                    options.ReminderIntervalSeconds = 5;
                    options.StorageProviderName = "GrainStreamStore";
                    options.CacheSize = 1024;
                });
        }
    }

    private sealed class ClientConfigurator : IClientBuilderConfigurator
    {
        public void Configure(IConfiguration configuration, IClientBuilder clientBuilder)
        {
            clientBuilder.AddGrainStream(StreamProviderName, options =>
            {
                options.NumQueues = 4;
                options.MaxBatchSize = 50;
                options.InFlightTimeoutSeconds = 10;
                options.ReminderIntervalSeconds = 5;
                options.StorageProviderName = "GrainStreamStore";
                options.CacheSize = 1024;
            });
        }
    }
}

[CollectionDefinition("ClusterCollection")]
public class ClusterCollection : ICollectionFixture<ClusterFixture>
{
}
