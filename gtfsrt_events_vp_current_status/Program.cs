using System.ServiceProcess;

namespace gtfsrt_events_vp_current_status
{
    internal static class Program
    {
        /// <summary>
        /// The main entry point for the application.
        /// </summary>
        private static void Main()
        {
            var ServicesToRun = new ServiceBase[]
                                {
                                    new gtfsrt_events_vp_current_status_service()
                                };
            ServiceBase.Run(ServicesToRun);
        }
    }
}
