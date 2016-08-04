using log4net;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace GTFS_realtime_service
{
    class EventQueue
    {
        private BlockingCollection<Event> eventQueue = new BlockingCollection<Event>();
        private AutoResetEvent queueNotifier = new AutoResetEvent(false);
        ILog Log;

        internal EventQueue(ILog log)
        {
            this.Log = log;
        }

        internal EventQueue()
        {
        }
        public void Enqueue(Event _event)
        {
            try
            {
                eventQueue.Add(_event);
                queueNotifier.Set();
            }
            catch(Exception e)
            {
                Log.Error(e.StackTrace);
            }
        }

        public Event Dequeue()
        {
            if(eventQueue.Count==0)
            {
                queueNotifier.WaitOne();
                queueNotifier.Reset();
            }
            Event _event = null;
            try
            {
                _event = eventQueue.Take();
            }
            catch(Exception e)
            {
                Log.Error(e.StackTrace);
            }
            queueNotifier.Reset();
            return _event;
        }




        internal int GetCount()
        {
            return eventQueue.Count;
        }
    }
}
