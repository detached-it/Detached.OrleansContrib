// -----------------------------------------------------------------------
// <copyright file="GrainStreamQueueAdapter.cs" company="Detached IT">
//     ©2026 Detached IT. All rights reserved
// </copyright>
// -----------------------------------------------------------------------

using Detached.OrleansContrib.Streaming.GrainStream.Configuration;
using Detached.OrleansContrib.Streaming.GrainStream.Grains;
using Detached.OrleansContrib.Streaming.GrainStream.Models;
using Microsoft.Extensions.Logging;
using Orleans.Streams;

namespace Detached.OrleansContrib.Streaming.GrainStream.Adapter;

/// <summary>
/// Queue adapter that delegates message operations to <see cref="IStreamQueueGrain"/> instances.
/// </summary>
public sealed class GrainStreamQueueAdapter(
    string name,
    IGrainFactory grainFactory,
    IStreamQueueMapper streamQueueMapper,
    GrainStreamOptions options,
    ILoggerFactory loggerFactory) : IQueueAdapter
{
    private readonly IGrainFactory _grainFactory = grainFactory ?? throw new ArgumentNullException(nameof(grainFactory));
    private readonly IStreamQueueMapper _streamQueueMapper = streamQueueMapper ?? throw new ArgumentNullException(nameof(streamQueueMapper));
    private readonly GrainStreamOptions _options = options ?? throw new ArgumentNullException(nameof(options));
    private readonly ILoggerFactory _loggerFactory = loggerFactory ?? throw new ArgumentNullException(nameof(loggerFactory));

    /// <inheritdoc/>
    public string Name { get; } = name ?? throw new ArgumentNullException(nameof(name));

    /// <inheritdoc/>
    public bool IsRewindable => false;

    /// <inheritdoc/>
    public StreamProviderDirection Direction => StreamProviderDirection.ReadWrite;

    /// <inheritdoc/>
    public IQueueAdapterReceiver CreateReceiver(QueueId queueId)
    {
        var grain = _grainFactory.GetGrain<IStreamQueueGrain>(queueId.ToString());
        return new GrainStreamQueueAdapterReceiver(
            grain,
            _options,
            _loggerFactory.CreateLogger<GrainStreamQueueAdapterReceiver>());
    }

    /// <inheritdoc/>
    public async Task QueueMessageBatchAsync<T>(
        StreamId streamId,
        IEnumerable<T> events,
        StreamSequenceToken? token,
        Dictionary<string, object> requestContext)
    {
        var queueId = _streamQueueMapper.GetQueueForStream(streamId);
        var grain = _grainFactory.GetGrain<IStreamQueueGrain>(queueId.ToString());

        var messages = events
            .Select(e => GrainStreamBatchContainer.ToMessage(streamId, e, requestContext))
            .ToList();

        await grain.EnqueueAsync(messages);
    }
}
