using log4net;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace gtfs_realtime_trip_service
{
    /*
     * This queue is used for queuing up the items in blocking
     * collection manner. Useful for multithreading.
     * 
     * */
    class BlockingQueue<T>
    {
        private BlockingCollection<T> _BlockingQueue = new BlockingCollection<T>();
        private AutoResetEvent QueueNotifier = new AutoResetEvent(false);
        private string QueueName;

        internal BlockingQueue(string queueName)
        {
            QueueName = queueName;
        }

        public BlockingQueue()
        {
            // TODO: Complete member initialization
        }

        internal string GetQueueName()
        {
            return this.QueueName;
        }

        public void Enqueue(T item)
        {
                _BlockingQueue.Add(item);
                QueueNotifier.Set();
        }

        public T Dequeue()
        {
            if (_BlockingQueue.Count == 0)
            {
                QueueNotifier.WaitOne();
                QueueNotifier.Reset();
            }
            T item = default(T);
            item = _BlockingQueue.Take();
            QueueNotifier.Reset();
            return item;
        }

        internal int GetCount()
        {
            return _BlockingQueue.Count;
        }
    }
}
