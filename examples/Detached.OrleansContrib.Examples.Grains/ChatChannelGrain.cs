using Orleans;
using Orleans.Runtime;
using Orleans.Streams;
using Detached.OrleansContrib.Examples.Contracts;

namespace Detached.OrleansContrib.Examples.Grains;

public class ChatChannelGrain : Grain, IChatChannelGrain
{
    private IAsyncStream<ChatMessage> _stream = null!;

    public override Task OnActivateAsync(CancellationToken cancellationToken)
    {
        var streamProvider = this.GetStreamProvider("ChatStreamProvider");
        _stream = streamProvider.GetStream<ChatMessage>(StreamId.Create("ChatNamespace", this.GetPrimaryKeyString()));
        return base.OnActivateAsync(cancellationToken);
    }

    public async Task Join(string userName)
    {
        await _stream.OnNextAsync(new ChatMessage("System", $"{userName} joined the channel.", DateTime.UtcNow));
    }

    public async Task Leave(string userName)
    {
        await _stream.OnNextAsync(new ChatMessage("System", $"{userName} left the channel.", DateTime.UtcNow));
    }

    public async Task SendMessage(string userName, string text)
    {
        await _stream.OnNextAsync(new ChatMessage(userName, text, DateTime.UtcNow));
    }
}
