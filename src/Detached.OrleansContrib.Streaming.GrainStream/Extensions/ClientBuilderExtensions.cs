// -----------------------------------------------------------------------
// <copyright file="ClientBuilderExtensions.cs" company="Detached IT">
//     ©2026 Detached IT. All rights reserved
// </copyright>
// -----------------------------------------------------------------------

using Detached.OrleansContrib.Streaming.GrainStream.Adapter;
using Detached.OrleansContrib.Streaming.GrainStream.Configuration;

namespace Detached.OrleansContrib.Streaming.GrainStream.Extensions;

/// <summary>
/// Extension methods for configuring the grain-based stream provider on a client.
/// </summary>
public static class ClientBuilderExtensions
{
    /// <summary>
    /// Adds a grain-based persistent stream provider to the client.
    /// </summary>
    /// <param name="builder">The client builder.</param>
    /// <param name="name">Provider name (must match the name used on the silo).</param>
    /// <param name="configure">Action to configure <see cref="GrainStreamOptions"/>.</param>
    /// <returns>The client builder for chaining.</returns>
    public static IClientBuilder AddGrainStream(
        this IClientBuilder builder,
        string name,
        Action<GrainStreamOptions>? configure = null)
    {
        builder.AddPersistentStreams(name, GrainStreamAdapterFactory.Create, stream =>
        {
            var options = new GrainStreamOptions();
            configure?.Invoke(options);

            stream.ConfigureStreamPubSub();
            stream.Configure<GrainStreamOptions>(ob => ob.Configure(o =>
            {
                o.NumQueues = options.NumQueues;
                o.MaxBatchSize = options.MaxBatchSize;
                o.InFlightTimeoutSeconds = options.InFlightTimeoutSeconds;
                o.ReminderIntervalSeconds = options.ReminderIntervalSeconds;
                o.StorageProviderName = options.StorageProviderName;
                o.CacheSize = options.CacheSize;
            }));
        });

        return builder;
    }
}
