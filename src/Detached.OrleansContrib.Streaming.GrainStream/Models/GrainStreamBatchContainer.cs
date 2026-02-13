// -----------------------------------------------------------------------
// <copyright file="GrainStreamBatchContainer.cs" company="Detached IT">
//     ©2026 Detached IT. All rights reserved
// </copyright>
// -----------------------------------------------------------------------

using System.Text.Json;
using Orleans.Providers.Streams.Common;
using Orleans.Streams;

namespace Detached.OrleansContrib.Streaming.GrainStream.Models;

/// <summary>
/// <see cref="IBatchContainer"/> implementation that wraps a <see cref="StreamMessage"/>.
/// Handles serialisation / deserialisation of the event payload.
/// </summary>
[GenerateSerializer]
[Alias("Detached.OrleansContrib.Streaming.GrainStream.GrainStreamBatchContainer")]
public sealed class GrainStreamBatchContainer : IBatchContainer
{
    /// <inheritdoc/>
    [Id(0)]
    public StreamId StreamId { get; set; }

    /// <summary>Serialised event payload.</summary>
    [Id(1)]
    public byte[] Payload { get; set; } = [];

    /// <summary>Monotonically increasing sequence number.</summary>
    [Id(2)]
    public long SequenceNumber { get; set; }

    /// <summary>Unique identifier for the message.</summary>
    [Id(3)]
    public Guid MessageId { get; set; }

    /// <summary>Optional request context propagated from the producer.</summary>
    [Id(4)]
    public Dictionary<string, object>? RequestContext { get; set; }

    StreamSequenceToken IBatchContainer.SequenceToken =>
        new EventSequenceTokenV2(SequenceNumber);

    /// <summary>
    /// Creates a <see cref="GrainStreamBatchContainer"/> from a <see cref="StreamMessage"/>.
    /// </summary>
    public static GrainStreamBatchContainer FromMessage(StreamMessage message)
    {
        ArgumentNullException.ThrowIfNull(message);
        return new GrainStreamBatchContainer
        {
            StreamId = message.StreamId,
            Payload = message.Payload,
            SequenceNumber = message.SequenceNumber,
            MessageId = message.MessageId,
            RequestContext = message.RequestContext
        };
    }

    /// <summary>
    /// Serialises an event (plus optional request context) into a byte[] payload.
    /// </summary>
    public static byte[] SerializeEvent<T>(T evt)
    {
        return JsonSerializer.SerializeToUtf8Bytes(evt);
    }

    /// <summary>
    /// Creates a complete <see cref="StreamMessage"/> ready for enqueuing.
    /// </summary>
    public static StreamMessage ToMessage<T>(StreamId streamId, T evt, Dictionary<string, object>? requestContext = null)
    {
        return new StreamMessage
        {
            StreamId = streamId,
            Payload = SerializeEvent(evt),
            RequestContext = requestContext
        };
    }

    /// <inheritdoc/>
    public IEnumerable<Tuple<T, StreamSequenceToken>> GetEvents<T>()
    {
        var evt = JsonSerializer.Deserialize<T>(Payload);
        if (evt is null) yield break;
        yield return Tuple.Create(evt, (StreamSequenceToken)new EventSequenceTokenV2(SequenceNumber));
    }

    /// <inheritdoc/>
    public bool ImportRequestContext()
    {
        if (RequestContext is null || RequestContext.Count == 0)
            return false;

        foreach (var kvp in RequestContext)
        {
            global::Orleans.Runtime.RequestContext.Set(kvp.Key, kvp.Value);
        }
        return true;
    }
}
