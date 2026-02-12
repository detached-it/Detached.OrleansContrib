// -----------------------------------------------------------------------
// <copyright file="GrainStreamQueueAdapterReceiver.cs" company="Detached IT">
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
/// Receives messages from a single <see cref="IStreamQueueGrain"/> partition.
/// Called periodically by the Orleans pulling agent.
/// </summary>
public sealed class GrainStreamQueueAdapterReceiver(
    IStreamQueueGrain queueGrain,
    GrainStreamOptions options,
    ILogger<GrainStreamQueueAdapterReceiver> logger) : IQueueAdapterReceiver
{
    private readonly IStreamQueueGrain _queueGrain = queueGrain ?? throw new ArgumentNullException(nameof(queueGrain));
    private readonly GrainStreamOptions _options = options ?? throw new ArgumentNullException(nameof(options));
    private readonly ILogger<GrainStreamQueueAdapterReceiver> _logger = logger ?? throw new ArgumentNullException(nameof(logger));

    /// <inheritdoc/>
    public Task Initialize(TimeSpan timeout)
    {
        _logger.LogDebug("GrainStreamQueueAdapterReceiver initialised.");
        return Task.CompletedTask;
    }

    /// <inheritdoc/>
    public async Task<IList<IBatchContainer>?> GetQueueMessagesAsync(int maxCount)
    {
        var effectiveMax = Math.Min(maxCount, _options.MaxBatchSize);
        var messages = await _queueGrain.DequeueAsync(effectiveMax);

        if (messages.Count == 0)
        {
            return null;
        }

        var batch = messages
            .Select(GrainStreamBatchContainer.FromMessage)
            .Cast<IBatchContainer>()
            .ToList();

        if (_logger.IsEnabled(LogLevel.Debug))
        {
            _logger.LogDebug("Retrieved {Count} messages from queue grain.", batch.Count);
        }
        return batch;
    }

    /// <inheritdoc/>
    public async Task MessagesDeliveredAsync(IList<IBatchContainer> messages)
    {
        if (messages is null || messages.Count == 0)
        {
            return;
        }

        var ids = messages
            .OfType<GrainStreamBatchContainer>()
            .Select(b => b.MessageId)
            .ToList();

        if (ids.Count > 0)
        {
            await _queueGrain.AcknowledgeAsync(ids);
            if (_logger.IsEnabled(LogLevel.Debug))
            {
                _logger.LogDebug("Acknowledged {Count} delivered messages.", ids.Count);
            }
        }
    }

    /// <inheritdoc/>
    public Task Shutdown(TimeSpan timeout)
    {
        _logger.LogDebug("GrainStreamQueueAdapterReceiver shutting down.");
        return Task.CompletedTask;
    }
}
