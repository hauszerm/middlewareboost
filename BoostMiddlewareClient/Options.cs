using CommandLine;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace BoostMiddlewareClient
{
    /// <summary>
    /// Defines the possible commandline arguments to this TCP client.
    /// </summary>
    public class Options {

        /// <summary>
        /// Default delay
        /// </summary>
        public const int DELAY_IN_MS = 10;

        [Option('s',  HelpText = "name or ip-adress of server, when not set, read from config file")] // Omitting long name, default --server
        public string Server { get; set;}

        [Option('p', DefaultValue = 0, HelpText = "tcp port of server, when not set, read from config file")] // Omitting long name, default --port
        public int Port { get; set; }

        [Option('c', DefaultValue=null,  HelpText = "number of client threads to create, when not set, user has to enter")]
        public int? ClientCount { get; set; }

        [Option('d', DefaultValue = DELAY_IN_MS, HelpText = "Delay in ms between two data packets")]
        public int Delay { get; set; }

        [Option('t', DefaultValue=1, HelpText="Number of tables for which we should generate data. Fill the TableName property of the tcp message in a round-robin manner. That way we can simulate concurrent table write for Cassandra and RavenDB.")]
        public int TargetTableCount { get; set; }

        [HelpOption]
        public string GetUsage()
        {
            return CommandLine.Text.HelpText.AutoBuild(this).ToString();
        }
    }
}
