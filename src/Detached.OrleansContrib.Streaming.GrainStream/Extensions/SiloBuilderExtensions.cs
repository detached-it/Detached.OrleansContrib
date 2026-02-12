// -----------------------------------------------------------------------
// <copyright file="SiloBuilderExtensions.cs" company="Detached IT">
//     ©2026 Detached IT. All rights reserved
// </copyright>
// -----------------------------------------------------------------------

using Detached.OrleansContrib.Streaming.GrainStream.Adapter;
using Detached.OrleansContrib.Streaming.GrainStream.Configuration;

namespace Detached.OrleansContrib.Streaming.GrainStream.Extensions;

/// <summary>
/// Extension methods for configuring the grain-based stream provider on a silo.
/// </summary>
public static class SiloBuilderExtensions
{
    /// <summary>
    /// Adds a grain-based persistent stream provider to the silo.
    /// </summary>
    /// <param name="builder">The silo builder.</param>
    /// <param name="name">Provider name (used with <c>[ImplicitStreamSubscription]</c> and <c>GetStreamProvider</c>).</param>
    /// <param name="configure">Action to configure <see cref="GrainStreamOptions"/>.</param>
    /// <returns>The silo builder for chaining.</returns>
    public static ISiloBuilder AddGrainStream(
        this ISiloBuilder builder,
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
