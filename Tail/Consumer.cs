using System;
using System.Threading;
using System.Threading.Tasks;
using System.Threading.Tasks.Dataflow;
using SqlStreamStore;
using SqlStreamStore.Subscriptions;

namespace Tail
{
    public class Consumer
    {
        private readonly int Id;
        private readonly IStreamStore Store;
        private readonly IScheduler Scheduler;
        private readonly BufferBlock<object> Mailbox;
        private readonly CancellationTokenSource MessagePumpCancellation;
        private readonly Task MessagePump;

        public Consumer(int id, IStreamStore store, IScheduler scheduler)
        {
            Id = id;
            Store = store ?? throw new ArgumentNullException(nameof(store));
            Scheduler = scheduler ?? throw new ArgumentNullException(nameof(scheduler));
            MessagePumpCancellation = new CancellationTokenSource();
            Mailbox = new BufferBlock<object>(new DataflowBlockOptions
            {
                BoundedCapacity = int.MaxValue,
                MaxMessagesPerTask = 1,
                CancellationToken = MessagePumpCancellation.Token
            });
            MessagePump = Task.Run(async () =>
            {
                try
                {
                    var random = new Random();
                    long? position = null;
                    IAllStreamSubscription subscription = null;
                    while (!MessagePumpCancellation.Token.IsCancellationRequested)
                    {
                        var message = await Mailbox.ReceiveAsync(MessagePumpCancellation.Token);
                        switch (message)
                        {
                            case SubscribeToAll subscribe:
                                Console.WriteLine("[{0}]SubscribeToAll", Id);
                                position = subscribe.ContinueAfter;
                                if (subscription != null) { subscription.Dispose(); }
                                subscription = Store.SubscribeToAll(
                                    subscribe.ContinueAfter,
                                    (_, received, token) =>
                                        Mailbox.SendAsync(new MessageReceived { Position = received.Position }),
                                    (_, reason, exception) =>
                                        Mailbox.SendAsync(new SubscriptionDropped { Reason = reason, Exception = exception })
                                );
                                break;
                            case MessageReceived received:
                                if(position.HasValue && position.Value > received.Position)
                                {
                                    Console.WriteLine("Received message at position {0} after message at position {1}.", received.Position, position.Value);
                                }
                                position = received.Position;
                                break;
                            case SubscriptionDropped dropped:
                                if (!MessagePumpCancellation.IsCancellationRequested)
                                {
                                    if (dropped.Exception == null)
                                    {
                                        Console.WriteLine("[{0}]Subscription dropped because {1}", Id, dropped.Reason);
                                    }
                                    else
                                    {
                                        Console.WriteLine("[{0}]Subscription dropped because {1}:{2}", Id, dropped.Reason, dropped.Exception);
                                    }
                                    Scheduler.ScheduleTellOnce(
                                        () => Mailbox.Post(new SubscribeToAll { ContinueAfter = position }),
                                        TimeSpan.FromMilliseconds(random.Next(100, 5000)) // consume resubscribes in between 100 and 5000ms
                                    );
                                    position = null;
                                }
                                break;
                        }
                    }
                }
                catch (OperationCanceledException) { }
            }, MessagePumpCancellation.Token);
        }

        public void Start() => Mailbox.Post(new SubscribeToAll { ContinueAfter = null });

        public void Stop() => MessagePumpCancellation.Cancel();

        private class SubscribeToAll { public long? ContinueAfter { get; set; } }

        private class MessageReceived { public long Position { get; set; } }

        private class SubscriptionDropped { public SubscriptionDroppedReason Reason { get; set; } public Exception Exception { get; set; } }
    }
}