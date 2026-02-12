using Orleans;

namespace Detached.OrleansContrib.Examples.Contracts;

[GenerateSerializer]
public record ChatMessage(string UserName, string Text, DateTime Timestamp);

public interface IChatChannelGrain : IGrainWithStringKey
{
    Task Join(string userName);

    Task Leave(string userName);

    Task SendMessage(string userName, string text);
}
