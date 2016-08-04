using log4net;
using log4net.Config;
using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Data;
using System.Diagnostics;
using System.Linq;
using System.Reflection;
using System.ServiceProcess;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace gtfs_realtime_trip_service
{
    public partial class Service1 : ServiceBase
    {
        private static ILog Log = LogManager.GetLogger(MethodBase.GetCurrentMethod().DeclaringType);

        public Service1()
        {
            InitializeComponent();
        }

        protected override void OnStart(string[] args)
        {
            try
            {
                XmlConfigurator.Configure();
                Log.Info("Program started");

                BlockingQueue<Event> InsertEventQueue = new BlockingQueue<Event>();
                BlockingQueue<Event> UpdateEventQueue = new BlockingQueue<Event>();

                DatabaseThread databaseThread = new DatabaseThread(Log, InsertEventQueue, UpdateEventQueue);
                Thread dataThread = new Thread(new ThreadStart(databaseThread.ThreadRun));
                dataThread.Start();

                Thread.Sleep(1000);

                EventRecorder eventRecorder = new EventRecorder(InsertEventQueue, UpdateEventQueue, Log);

                Thread eventThread = new Thread(new ThreadStart(eventRecorder.RecordEvents));
                eventThread.Start();
            }
            catch (Exception e)
            {
                Log.Error(e.Message);
                Log.Error(e.InnerException);
                Log.Error(e.StackTrace);

                Thread.Sleep(1000);
                Environment.Exit(1);
            }
        }

        protected override void OnStop()
        {
            Thread.Sleep(1000);
            Environment.Exit(0);
        }
    }
}
