using System;
using System.Collections.Immutable;
using System.Threading;
using System.Threading.Tasks;
using System.Threading.Tasks.Dataflow;
using NodaTime;

namespace Tail
{
    public class Scheduler : IScheduler, IDisposable
    {
        private const int TimerFrequency = 100;

        private IClock Clock { get; }
        private BufferBlock<object> Mailbox { get; }
        private Timer Timer { get; }
        private CancellationTokenSource MessagePumpCancellation { get; }
        private Task MessagePump { get; }

        private class ScheduledAction
        {
            public Action Action { get; set; }
            public Instant Due { get; set; }
        }

        private ImmutableList<ScheduledAction> ScheduledActions { get; set; } = ImmutableList<ScheduledAction>.Empty;
        
        public Scheduler(IClock clock)
        {
            Clock = clock ?? throw new ArgumentNullException(nameof(clock));
            Mailbox = new BufferBlock<object>();            
            Timer = new Timer(_ => 
            {
                Mailbox.Post(new Messages.TimerElapsed { Time = clock.GetCurrentInstant() });
            }, null, 0, TimerFrequency);
            MessagePumpCancellation = new CancellationTokenSource();
            MessagePump = Task.Run(async() =>
            {
                while(!MessagePumpCancellation.IsCancellationRequested)
                {
                    var message = await Mailbox.ReceiveAsync(MessagePumpCancellation.Token);
                    switch (message)
                    {
                        case Messages.ScheduleTellOnce msg:
                            ScheduledActions = ScheduledActions.Add(new ScheduledAction { Action = msg.Action, Due = msg.Due });
                            break;
                        case Messages.TimerElapsed msg:
                            var due = ScheduledActions.FindAll(candidate => candidate.Due <= msg.Time);
                            due.ForEach(match => match.Action());
                            ScheduledActions = ScheduledActions.RemoveRange(due);
                            break;
                    }
                }
            }, MessagePumpCancellation.Token);
        }

        private class Messages
        {
            public class ScheduleTellOnce { public Action Action { get; set; } public Instant Due { get; set; } }

            public class TimerElapsed { public Instant Time { get; set; } }
        }
        
        public void ScheduleTellOnce(Action action, TimeSpan due)
        {
            Mailbox.Post(new Messages.ScheduleTellOnce { Action = action, Due = Clock.GetCurrentInstant().Plus(Duration.FromTimeSpan(due)) });
        }

        public Task ScheduleTellOnceAsync(Action action, TimeSpan due, CancellationToken token = default)
        {
            return Mailbox.SendAsync(new Messages.ScheduleTellOnce { Action = action, Due = Clock.GetCurrentInstant().Plus(Duration.FromTimeSpan(due)) }, token);
        }

        public Task Disposed => Mailbox.Completion;

        public void Dispose()
        {
            MessagePumpCancellation.Cancel();
            MessagePumpCancellation.Dispose();
            Timer.Change(Timeout.Infinite, Timeout.Infinite);
            Timer.Dispose();
            Mailbox.Complete();
            if(MessagePump.IsCanceled || MessagePump.IsFaulted || MessagePump.IsCompleted)
            {
                MessagePump.Dispose();
            }
        }
    }
}
