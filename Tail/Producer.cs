using System;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using System.Threading.Tasks.Dataflow;
using Bogus.DataSets;
using SqlStreamStore;
using SqlStreamStore.Streams;

namespace Tail
{
    public class Producer
    {
        private readonly IStreamStore Store;
        private readonly IScheduler Scheduler;
        private readonly BufferBlock<object> Mailbox;
        private readonly CancellationTokenSource MessagePumpCancellation;
        private readonly Task MessagePump;

        public Producer(int id, IStreamStore store, IScheduler scheduler)
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
                var text = new Lorem();
                var random = new Random();
                while(!MessagePumpCancellation.Token.IsCancellationRequested)
                {
                    var message = await Mailbox.ReceiveAsync(MessagePumpCancellation.Token);
                    switch(message)
                    {
                        case AppendToStream append:
                            var messages = Enumerable
                                    .Range(0, random.Next(1, 100)) // produce between 1 and 99 messages per append
                                    .Select(index => new NewStreamMessage(Guid.NewGuid(), append.Stream, text.Sentences(random.Next(5, 10)))) //randomize the data a bit
                                    .ToArray();

                            var result = await Store.AppendToStream(
                                append.Stream, append.ExpectedVersion,
                                messages,
                                MessagePumpCancellation.Token);

                            await scheduler.ScheduleTellOnceAsync(
                                () => Mailbox.Post(new AppendToStream { Stream = append.Stream, ExpectedVersion = result.CurrentVersion }),
                                TimeSpan.FromMilliseconds(random.Next(100, 5000)), // produce another append on the same stream in about 100 to 5000ms
                                MessagePumpCancellation.Token
                            );
                            break;
                    }
                }
            }, MessagePumpCancellation.Token);
        }

        public int Id { get; }

        public void Start() => Mailbox.Post(new AppendToStream { Stream = "producer-" + Id, ExpectedVersion = ExpectedVersion.NoStream });
        public void Stop() => MessagePumpCancellation.Cancel();

        private class AppendToStream
        {
            public string Stream { get; set; }
            public int ExpectedVersion { get; set; }
        }
    }
}
