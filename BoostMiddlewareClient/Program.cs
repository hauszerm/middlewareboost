using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Runtime.CompilerServices;
using System.Net;
using System.Net.Sockets;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using System.Configuration;
using System.Net.NetworkInformation;
using System.Diagnostics;
using BoostMiddlewareObjects;
using Newtonsoft.Json;



namespace BoostMiddlewareClient
{

    /// <summary>
    /// TCP Client
    /// 
    /// CommandLine Arguments:
    ///   see class <see cref="Options"/>
    ///   
    /// Sends JSON messages to the TCP Server representing a <see cref="PlantData"/> object.
    /// 
    /// configuration parameters
    ///   see App.config
    ///   
    /// Program structure
    ///   Creates a separate thread for each tcp client connection to the tcp server
    ///   and periodically sends JSON messages to the tcp server
    ///   
    /// test modes:
    ///   * by default, each client thread sends in an interval of <see cref="Options.DELAY_IN_MS"/> (10ms) the TCP message
    ///   ** <see cref="Options"/> to configure that value
    ///   
    ///   * a TCP message can contain a list of n PlantData Objects where each object will be converted to a datastore record
    ///   ** see app.config to configure the number
    ///   
    ///   * a TCP message can contain a TableName-number that tells the server in which table
    ///     the plantdata should be inserted, so we can test with a defined number of concurrent table writes.
    ///     
    ///   * this app can immediately start with a given number of clients without userinteraction
    ///   ** <see cref="Options"/> to configure the value
    /// </summary>
    class Program
    {
        private static bool running = true;

        private static string _filldata;

        /// <summary> 
        /// This utility function displays all the IP (v4, not v6) addresses of the local computer. 
        /// </summary> 
        public static IPAddress GetLocalActiveIPAddress()
        {
            IPAddress ipAddress = IPAddress.Loopback; ;

            // Get a list of all network interfaces (usually one per network card, dialup, and VPN connection) 
            NetworkInterface[] networkInterfaces = NetworkInterface.GetAllNetworkInterfaces();

            foreach (NetworkInterface network in networkInterfaces)
            {
                // Read the IP configuration for each network 
                IPInterfaceProperties properties = network.GetIPProperties();
                if (network.OperationalStatus == OperationalStatus.Up)
                {

                    // Each network interface may have multiple IP addresses 
                    foreach (IPAddressInformation address in properties.UnicastAddresses)
                    {
                        // We're only interested in IPv4 addresses for now 
                        if (address.Address.AddressFamily == AddressFamily.InterNetwork)
                        {
                            ipAddress = address.Address;
                            break;
                        }
                    }
                }
            }
            return ipAddress;
        }

        static void Main(string[] args)
        {
            var options = new Options();
            if (CommandLine.Parser.Default.ParseArguments(args, options))
            {
                Run(options);
            }
            else
            {
                // Display the default usage information
                Console.WriteLine(options.GetUsage());
            }
        }

        private static void Run(Options options)
        {
            #region tests
            //var delay = Task.Run(async () =>
            //{
            //    Stopwatch sw = Stopwatch.StartNew();
            //    await Task.Delay(1000);
            //    sw.Stop();
            //    return sw.ElapsedMilliseconds;
            //});
            //Console.WriteLine("(1) Elapsed milliseconds: {0}", delay.Result);

            //Stopwatch sw2 = Stopwatch.StartNew();
            //delay = Task.Delay(1000).ContinueWith(_ =>
            //{
            //    sw2.Stop();
            //    return sw2.ElapsedMilliseconds;
            //});
            //Console.WriteLine("(2) Elapsed milliseconds: {0}", delay.Result);
            #endregion

            if (options.TargetTableCount < 1)
            {
                //less than one table is not possible
                options.TargetTableCount = 1;
            }

            if (options.ClientCount == null) { 
                Console.Write("Anzahl der Clients: ");
                string input = Console.ReadLine();
                int countClients = 0;
                if (!int.TryParse(input, out countClients))
                {
                    countClients = 3;
                }

                options.ClientCount = countClients;
            }

            if (String.IsNullOrEmpty(options.Server))
            {
                //StartClient(args[0], Convert.ToInt32(args[1]));
                options.Server = ConfigurationManager.AppSettings["Server"];
            }

            if (options.Port == 0)
            {
                options.Port = Convert.ToInt32(ConfigurationManager.AppSettings["Port"]);
            }

            _filldata = ConfigurationManager.AppSettings["Filldata"];

            int measurementsPerMessage = 0;
            if (!Int32.TryParse(ConfigurationManager.AppSettings["MeasurementsPerMessage"], out measurementsPerMessage)) {
                measurementsPerMessage = 0;
            }

            /*
             * start that number of client immediately 
             */
            for (int i = 0; i < options.ClientCount; i++)
            {
                int currentIndex = i;
                 
                Task task = Task.Factory.StartNew(() =>
                {
                    StartClient(options.Server, options.Port, 20 + (currentIndex * 0.5f) + (currentIndex * 0.01f), options.Delay, measurementsPerMessage, options.TargetTableCount);
                }
                ,TaskCreationOptions.LongRunning //if set, informs the scheduler that oversubscription may be warranted
                );

                //task.Start(); //already done by StartNew()
                //task.Wait(); //Wenn Wait hier wegfällt, ändert sich am Programm nichts
            }

            Console.ReadKey();
            running = false;
            Console.WriteLine("endgültig stoppen?");
            Console.ReadKey();
        }


        /// <summary>
        /// 
        /// </summary>
        /// <param name="server"></param>
        /// <param name="port"></param>
        /// <param name="startTemp2"></param>
        /// <param name="delay">milliseconds between two messages</param>
        /// <param name="measurementsPerMessage">measurements added to each message</param>
        private static async void StartClient(string server, int port, float startTemp2, int delay, int measurementsPerMessage, int targetTableCount)
        {
            IPAddress ipAddress = IPAddress.Loopback;

            IPHostEntry host = Dns.GetHostEntry(server);
            string hostname = Dns.GetHostName();
            if (!server.Equals(hostname))
            {
                foreach (IPAddress ip in host.AddressList)
                {
                    if (ip.AddressFamily == AddressFamily.InterNetwork)
                    {
                        ipAddress = ip;
                        LogMessage("Ip = " + ip.ToString());
                    }
                }
            }
            TcpClient client = null;
            int bytecount = 0;
            Stopwatch sw2 = new Stopwatch();
            Stopwatch sw_cycle = new Stopwatch();
            long elapsedMillisecondsSum = 0;
            string clientInfo = string.Empty;

            

            /*
             *   Temperatur2 beginnt mit diesem Wert, geht dann in 0.5 Schritten nach oben (um 10°)
             *   danach in 0.5 Schritten runter auf einen Wert um 10° niedriger als der Startwert
             *   danach wieder nach oben  
             */
            float temp2 = startTemp2;
            bool increaseTemp = true;

            double messagesPerSecond = 1000.0 / delay;

            try
            {
                client = new TcpClient();
                await client.ConnectAsync(ipAddress, port);
                clientInfo = client.Client.LocalEndPoint.ToString()
                                   .Split(':').Skip(1).First(); //extract port
                LogMessage("Client " + clientInfo + " connected to Server");

                UTF8Encoding encoding = new UTF8Encoding();
                MemoryStream memStream = null;

                using (var networkStream = client.GetStream())
                //using (var writer = new StreamWriter(networkStream))
                //using (var reader = new StreamReader(networkStream))
                {
                    //writer.AutoFlush = true;

                    int loop = 0;
                    long plantDataCount = 0;
                    while (running)
                    {
                        loop++;

                        int measurementId = 1;

                        PlantData plantData = new PlantData()
                        {
                            PlantAddress = clientInfo,
                            MeasurementId = measurementId++,

                            TargetTable = plantDataCount++ % targetTableCount,
                            TimestampClient = DateTime.UtcNow,
                            Temp1 = 50 + (Thread.CurrentThread.ManagedThreadId / 10.0f),
                            Temp2 = temp2,
                            FillData = _filldata,
                            Thread = Thread.CurrentThread.ManagedThreadId,
                            
                        };

                        //include more plantdata to one tcp message
                        plantData.MeasurmentList = new List<PlantData>(measurementsPerMessage);
                        List<PlantData> mList = plantData.MeasurmentList;
                        for (int i = 0; i < measurementsPerMessage; i++)
                        {
                            mList.Add(new PlantData()
                            {
                                PlantAddress = clientInfo,
                                MeasurementId = measurementId++,

                                TargetTable = plantDataCount++ % targetTableCount,
                                Temp1 = plantData.Temp1 + i,
                                Temp2 = plantData.Temp2,
                                Thread = plantData.Thread,
                                TimestampClient = plantData.TimestampClient
                            });
                        }

                        string input = JsonConvert.SerializeObject(plantData, Formatting.None);
                        //bytecount += input.Length;
                        bytecount = input.Length;

                        PlantData deserializeTest = JsonConvert.DeserializeObject<PlantData>(input);

                        byte[] data = encoding.GetBytes(input);
                        memStream = new MemoryStream();
                        memStream.Write(BitConverter.GetBytes(data.Length), 0, 4); //send length of bytes that will be sent
                        memStream.Write(data, 0, data.Length); //send real data
                        byte[] buffer = memStream.ToArray();

                        // add data length (32bit integer)
                        bytecount += 4;

                        await networkStream.WriteAsync(buffer, 0, buffer.Length);

                        if (loop > 0 && (messagesPerSecond <= 1 || (loop % messagesPerSecond == 0)))
                        {
                            double elapsedMillisecondsAvg = (double)elapsedMillisecondsSum;

                            if (messagesPerSecond > 1)
                            {
                                elapsedMillisecondsAvg = elapsedMillisecondsAvg / messagesPerSecond;
                            }

                            LogMessage("client " + clientInfo + " sent " + bytecount.ToString() + " bytes, delay=" + ToHumanReadableMilliseconds(elapsedMillisecondsAvg) + " loopTime=" + ToHumanReadableMilliseconds(sw_cycle.ElapsedMilliseconds));
                            elapsedMillisecondsSum = 0;

                            #region KB

                            // Task.Delay ist zu ungenau für kurze Zeiten
                            //await Task.Run(async delegate
                            //{
                            //    //Console.WriteLine("start waiting");
                            //    Stopwatch sw = Stopwatch.StartNew();
                            //    await Task.Delay(1);
                            //    sw.Stop();
                            //    //Console.WriteLine("end waiting");
                            //    Console.WriteLine("Task.Delay elapsed milliseconds: {0}", sw.ElapsedMilliseconds);
                            //    LogMessage("Task.Delay elapsed milliseconds: "+  sw.ElapsedMilliseconds.ToString());
                            //});

                            // Thread Sleep ist genauer
                            //Stopwatch sw = Stopwatch.StartNew();
                            //Thread.Sleep(1);
                            //sw.Stop();
                            #endregion


                            if (increaseTemp && (temp2 < startTemp2 + 10)) {
                                //increase temperature
                                temp2 = temp2 + 0.5f;
                            }
                            else if ((!increaseTemp) && (temp2 > startTemp2 - 10))
                            {
                                //decrease temperature
                                temp2 = temp2 - 0.5f;
                            }
                            else
                            {
                                //inverse direction
                                increaseTemp = !increaseTemp;
                            }
                        }

                        sw_cycle.Restart();

                        sw2.Restart();
                        //await Task.Delay(delay); //ist zu ungenau für kurze Zeiten
                        Thread.Sleep(delay); //Ist aber ein Performance Problem, da der Thread für andere Tasks nicht zur Verfügung steht
                        
                        sw2.Stop();
                        elapsedMillisecondsSum += sw2.ElapsedMilliseconds;
                        #region cleanup

                        //LogMessage("Task.Delay elapsed milliseconds: " + sw2.ElapsedMilliseconds.ToString());

                        /*
                            string[] commands = input.Split(' ');
                            if (commands[0] == "data")
                            {
                                // send file
                                using (FileStream fs = new FileStream(fileName, FileMode.Open, FileAccess.Read))
                                {
                                    int sentBytes = 0;
                                    using (BinaryReader br = new BinaryReader(fs))
                                    {
                                        //byte[] buffer = br.ReadBytes(BUFFER_SIZE);
                                        // IMPORTANT: file may not exceed 4GB bytes
                                        byte[] buffer = br.ReadBytes((int)fs.Length);
                                        while (buffer.Length > 0)
                                        {
                                            sentBytes += buffer.Length;
                                            await networkStream.WriteAsync(buffer, 0, buffer.Length);
                                            LogMessage("sent " + buffer.Length.ToString() + " bytes over network");
                                            buffer = br.ReadBytes(BUFFER_SIZE);
                                        }
                                    }
                                    LogMessage("finished sending " + sentBytes.ToString() + " bytes over network");

                                }

                            }
                        */
                        #endregion
                    } // end 
                } // end 
            }
            catch (Exception exp)
            {
                LogMessage(exp.Message.ToString());
            }
            finally
            {
                if (client != null)
                {
                    LogMessage("client " + clientInfo + " sent " + bytecount.ToString() + " bytes");
                    client.Close();
                }

            }
        }


        private static string ToHumanReadableMilliseconds(double milliseconds)
        {
            if (milliseconds < 1000) {
                return milliseconds.ToString("000.00") + "ms";
            }

            milliseconds = milliseconds / 1000.0;
            return milliseconds.ToString("000.00") + "s ";
        }//StartClient


        private static void LogMessage(string message, [CallerMemberName]string callername = "")
        {
            callername += " - ";

            callername = String.Empty; //we have no space for the callername to log, thk 09.20.2016
            System.Console.WriteLine("{0}Thread-{1:00}- {2}",
                callername, Thread.CurrentThread.ManagedThreadId, message);
        }

    }
}


