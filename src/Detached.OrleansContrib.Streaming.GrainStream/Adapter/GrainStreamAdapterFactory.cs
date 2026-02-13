// -----------------------------------------------------------------------
// <copyright file="GrainStreamAdapterFactory.cs" company="Detached IT">
//     ©2026 Detached IT. All rights reserved
// </copyright>
// -----------------------------------------------------------------------

using Detached.OrleansContrib.Streaming.GrainStream.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Orleans.Configuration;
using Orleans.Providers.Streams.Common;
using Orleans.Streams;

namespace Detached.OrleansContrib.Streaming.GrainStream.Adapter;

/// <summary>
/// Factory for creating <see cref="GrainStreamQueueAdapter"/> instances.
/// </summary>
public sealed class GrainStreamAdapterFactory : IQueueAdapterFactory
{
    private readonly string _providerName;
    private readonly GrainStreamOptions _options;
    private readonly IGrainFactory _grainFactory;
    private readonly ILoggerFactory _loggerFactory;
    private readonly IStreamQueueMapper _streamQueueMapper;
    private readonly IQueueAdapterCache _adapterCache;

    /// <summary>
    /// Initializes a new instance of the <see cref="GrainStreamAdapterFactory"/> class.
    /// </summary>
    /// <param name="name">The provider name.</param>
    /// <param name="options">Stream configuration options.</param>
    /// <param name="grainFactory">Orleans grain factory.</param>
    /// <param name="loggerFactory">Logger factory.</param>
    /// <param name="queueMapperOptions">Queue mapper options.</param>
    /// <param name="cacheOptions">Cache options.</param>
    public GrainStreamAdapterFactory(
        string name,
        GrainStreamOptions options,
        IGrainFactory grainFactory,
        ILoggerFactory loggerFactory,
        HashRingStreamQueueMapperOptions queueMapperOptions,
        SimpleQueueCacheOptions cacheOptions)
    {
        _providerName = name ?? throw new ArgumentNullException(nameof(name));
        _options = options ?? throw new ArgumentNullException(nameof(options));
        _grainFactory = grainFactory ?? throw new ArgumentNullException(nameof(grainFactory));
        _loggerFactory = loggerFactory ?? throw new ArgumentNullException(nameof(loggerFactory));

        _streamQueueMapper = new HashRingBasedStreamQueueMapper(queueMapperOptions, _providerName);
        _adapterCache = new SimpleQueueAdapterCache(cacheOptions, _providerName, _loggerFactory);
    }

    /// <summary>
    /// Creates the factory via the DI-based static factory pattern expected by Orleans.
    /// </summary>
    public static GrainStreamAdapterFactory Create(IServiceProvider services, string name)
    {
        var options = services.GetOptionsByName<GrainStreamOptions>(name);
        var grainFactory = services.GetRequiredService<IGrainFactory>();
        var loggerFactory = services.GetRequiredService<ILoggerFactory>();

        var queueMapperOptions = new HashRingStreamQueueMapperOptions { TotalQueueCount = options.NumQueues };
        var cacheOptions = new SimpleQueueCacheOptions { CacheSize = options.CacheSize };

        return new GrainStreamAdapterFactory(name, options, grainFactory, loggerFactory, queueMapperOptions, cacheOptions);
    }

    /// <inheritdoc/>
    public Task<IQueueAdapter> CreateAdapter()
    {
        var adapter = new GrainStreamQueueAdapter(
            _providerName,
            _grainFactory,
            _streamQueueMapper,
            _options,
            _loggerFactory);

        return Task.FromResult<IQueueAdapter>(adapter);
    }

    /// <inheritdoc/>
    public IQueueAdapterCache GetQueueAdapterCache() => _adapterCache;

    /// <inheritdoc/>
    public IStreamQueueMapper GetStreamQueueMapper() => _streamQueueMapper;

    /// <inheritdoc/>
    public Task<IStreamFailureHandler> GetDeliveryFailureHandler(QueueId queueId) =>
        Task.FromResult<IStreamFailureHandler>(new NoOpStreamDeliveryFailureHandler());
}
