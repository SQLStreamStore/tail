using System;
using System.Threading;
using System.Threading.Tasks;
using System.Threading.Tasks.Dataflow;
using SqlStreamStore;

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
                EnsureOrdered = true,
                BoundedCapacity = int.MaxValue,
                MaxMessagesPerTask = 1,
                CancellationToken = MessagePumpCancellation.Token
            });
            MessagePump = Task.Run(async() =>
            {
                var random = new Random();
                long? position = null;
                IAllStreamSubscription subscription = null;
                while(!MessagePumpCancellation.Token.IsCancellationRequested)
                {
                    var message = await Mailbox.ReceiveAsync(MessagePumpCancellation.Token);
                    switch(message)
                    {
                        case SubscribeToAll subscribe:
                            position = null;
                            if(subscription != null) { subscription.Dispose(); }
                            subscription = Store.SubscribeToAll(
                                null,
                                (_, received, token) => {
                                    if(position > received.Position)
                                    {
                                        Console.WriteLine("[{0}]Observed {1} after {2}", Id, received.Position, position);
                                    }
                                    position = received.Position;
                                    return Task.CompletedTask;
                                },
                                (_, reason, exception) => {
                                    Console.WriteLine("[{0}]Subscription dropped because {1}:{2}", Id, reason, exception);
                                    Scheduler.ScheduleTellOnce(
                                        () => Mailbox.Post(new SubscribeToAll { ContinueAfter = null }),
                                        TimeSpan.FromMilliseconds(random.Next(100, 5000)) // consume resubscribes in between 100 and 5000ms
                                    );
                                }
                            );
                            break;
                    }
                }
            }, MessagePumpCancellation.Token);
        }

        public void Start() => Mailbox.Post(new SubscribeToAll { ContinueAfter = null });

        public void Stop() => MessagePumpCancellation.Cancel();

        private class SubscribeToAll { public long? ContinueAfter { get; set; } }
    }
}