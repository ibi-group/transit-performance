using log4net;
using log4net.Config;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using System.Text;
using System.Threading.Tasks;

namespace GTFS
{
    class Program
    {
        private static ILog Log = LogManager.GetLogger(MethodBase.GetCurrentMethod().DeclaringType);

        private static int Main(string[] args)
        {
            try
            {
                XmlConfigurator.Configure();

                Log.Info("\n\nGTFS schedule update program start.");

                GTFSUpdate gtfsUpdate = new GTFSUpdate();

                Log.Info("Initialising GTFS update");
                bool initialisationSuccessful = gtfsUpdate.InitialiseGTFSUpdate(Log);

                if (initialisationSuccessful)
                {
                    Log.Info("Initialisation of GTFS update successful.");
                    Log.Info("Running GTFS update");
                    int runningSuccessful = gtfsUpdate.RunGTFSUpdate();
                    if (runningSuccessful == 0)
                    {

                        Log.Info("Begin Migrating process.");
                        GTFSMigrateProcess gtfsMigrateProcess = new GTFSMigrateProcess();
                        bool migrationSuccessful = gtfsMigrateProcess.BeginMigration(Log);
                        if (migrationSuccessful)
                        {
                            Log.Info("GTFS migration successful");
                            Log.Info("GTFS Schedule update successful.");
                            Log.Info("GTFS schedule update program end.\n\n");
                            return 0;
                        }
                        else
                        {
                            Log.Info("GTFS migration failed");
                            Log.Info("GTFS Schedule update failed.");
                            Log.Info("GTFS schedule update program end.\n\n");
                            return 1;
                        }
                    }
                    else
                    {
                        if (runningSuccessful == 1)
                        {
                            Log.Info("GTFS Schedule update failed.");
                            Log.Info("GTFS schedule update program end.\n\n");
                        }
                        return 1;
                    }
                }
                else
                {
                    Log.Info("Initialisation of GTFS update failed.");
                    Log.Info("GTFS schedule update program end.\n\n");
                    return 1;
                }

                //Log.Info("GTFS schedule update program end.\n\n");
            }
            catch (Exception e)
            {
                Log.Error(e.Message);
                Log.Error(e.StackTrace);
                return 1;
            }
        }
    }
}
