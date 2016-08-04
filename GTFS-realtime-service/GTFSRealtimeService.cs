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

namespace GTFS_realtime_service
{
    public partial class GTFSRealtimeService : ServiceBase
    {
        private static ILog Log = LogManager.GetLogger(MethodBase.GetCurrentMethod().DeclaringType);

        public GTFSRealtimeService()
        {
            InitializeComponent();
        }

        protected override void OnStart(string[] args)
        {
            StartGTFSRealtimeService();
        }

        private void StartGTFSRealtimeService()
        {
            try
            {
                XmlConfigurator.Configure();
                Log.Info("Start");
                EventQueue eventQueue = new EventQueue(Log);
                DatabaseThread databaseThread = new DatabaseThread(Log, eventQueue);
                Thread dataThread = new Thread(new ThreadStart(databaseThread.ThreadRun));
                dataThread.Start();
                Thread.Sleep(1000);
                EventRecorder eventRecorder = new EventRecorder(eventQueue, Log);
                Thread eventThread = new Thread(new ThreadStart(eventRecorder.RecordEvents));
                eventThread.Start();
            }
            catch(Exception e)
            {
                Log.Error(e.Message);
                Log.Error(e.StackTrace);
                Environment.Exit(1);
            }
        }

        protected override void OnStop()
        {
            Log.Info("Stop");
            Environment.Exit(0);
        }
    }
}
