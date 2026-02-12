// -----------------------------------------------------------------------
// <copyright file="InFlightMessage.cs" company="Detached IT">
//     ©2026 Detached IT. All rights reserved
// </copyright>
// -----------------------------------------------------------------------

namespace Detached.OrleansContrib.Streaming.GrainStream.Models;

/// <summary>
/// Wraps a <see cref="StreamMessage"/> with delivery tracking metadata.
/// </summary>
[GenerateSerializer]
[Alias("Detached.OrleansContrib.Streaming.GrainStream.Models.InFlightMessage")]
public sealed class InFlightMessage
{
    /// <summary>The original message.</summary>
    [Id(0)]
    public StreamMessage Message { get; set; } = null!;

    /// <summary>UTC timestamp when the message was handed to the pulling agent.</summary>
    [Id(1)]
    public DateTime DeliveredAtUtc { get; set; } = DateTime.UtcNow;
}
