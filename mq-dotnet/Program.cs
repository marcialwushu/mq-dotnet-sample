using IBM.XMS;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace mq_dotnet
{
    public class Progran
    {
        private IConnection connectionWMQ = null;
        private IDictionary<string, object> properties = null;
        private List<double> myarray = new List<double>();
        private ISession sessionWMQ = null;
        private Stopwatch timer;
        private int numberOfMsgs = 1;
        private bool complete = false;

        public void Publish()
        {
            try
            {
                // Create session
                using (sessionWMQ = connectionWMQ.CreateSession(false, AcknowledgeMode.AutoAcknowledge))
                {
                    // Create destination
                    var destination = sessionWMQ.CreateQueue((string)properties[XMSC.WMQ_QUEUE_NAME]);

                    // Create producer
                    var producer = sessionWMQ.CreateProducer(destination);

                    var msgType = Convert.ToInt32(properties["Persistence"]);
                    if (msgType == 0)
                        producer.DeliveryMode = DeliveryMode.NonPersistent;
                    else if (msgType == 1)
                        producer.DeliveryMode = DeliveryMode.Persistent;
                    else
                        producer.DeliveryMode = DeliveryMode.NonPersistent;

                    var msgSize = Convert.ToInt32(properties["MessageSize"]);
                    var str = new String('*', msgSize);
                    timer = new Stopwatch();

                    numberOfMsgs = Convert.ToInt32(properties["NoofMessages"]);
                    for (int i = 0; i < 5000; ++i)
                    {
                        var textMessage = sessionWMQ.CreateTextMessage();
                        textMessage.Text = str;
                        producer.Send(textMessage);
                    }
                    timer.Start();
                    for (int i = 0; i < numberOfMsgs; ++i)
                    {
                        // Create a text message and send it.
                        var textMessage = sessionWMQ.CreateTextMessage();
                        textMessage.Text = str;
                        producer.Send(textMessage);
                    }
                    timer.Stop();
                    Console.WriteLine("Time taken by " + Task.CurrentId + " is :" + timer.Elapsed.TotalSeconds);
                    myarray.Add(timer.Elapsed.TotalSeconds);
                }
            }
            catch (XMSException ex)
            {
                Console.WriteLine("XMSException caught: {0}", ex);
                if (ex.LinkedException != null)
                {
                    Console.WriteLine("Stack Trace:\n {0}", ex.LinkedException.StackTrace);
                }
                Console.WriteLine("Sample execution  FAILED!");
            }
        }

        public void Consume()
        {
            try
            {
                XMSFactoryFactory factoryFactory;
                IConnectionFactory cf;

                factoryFactory = XMSFactoryFactory.GetInstance(XMSC.CT_WMQ);
                cf = factoryFactory.CreateConnectionFactory();

                cf.SetStringProperty(XMSC.WMQ_HOST_NAME, (String)properties[XMSC.WMQ_HOST_NAME]);
                cf.SetIntProperty(XMSC.WMQ_PORT, Convert.ToInt32(properties[XMSC.WMQ_PORT]));
                cf.SetStringProperty(XMSC.WMQ_CHANNEL, (String)properties[XMSC.WMQ_CHANNEL]);
                cf.SetIntProperty(XMSC.WMQ_CONNECTION_MODE, XMSC.WMQ_CM_CLIENT);
                cf.SetStringProperty(XMSC.WMQ_QUEUE_MANAGER, (String)properties[XMSC.WMQ_QUEUE_MANAGER]);

                connectionWMQ = cf.CreateConnection();

                using (sessionWMQ = connectionWMQ.CreateSession(true, AcknowledgeMode.AutoAcknowledge))
                {
                    var destination = sessionWMQ.CreateQueue((string)properties[XMSC.WMQ_QUEUE_NAME]);

                    var consumerAsync = sessionWMQ.CreateConsumer(destination);

                    var messageListener = new MessageListener(OnMessageCallback);
                    consumerAsync.MessageListener = messageListener;
                    
                    timer = new Stopwatch();

                    connectionWMQ.Start();

                    Console.ReadKey();
                    connectionWMQ.Stop();

                    // Cleanup
                    consumerAsync.Close();
                    connectionWMQ.Close();
                }
            }
            catch (XMSException ex)
            {
                Console.WriteLine("XMSException caught: {0}", ex);
                if (ex.LinkedException != null)
                {
                    Console.WriteLine("Stack Trace:\n {0}", ex.LinkedException.StackTrace);
                }
                Console.WriteLine("Sample execution  FAILED!");
            }
        }

        public IConnection Connection()
        {
            // Get an instance of factory.
            var factoryFactory = XMSFactoryFactory.GetInstance(XMSC.CT_WMQ);

            // Create WMQ Connection Factory.
            var cf = factoryFactory.CreateConnectionFactory();

            // Set the properties
            cf.SetStringProperty(XMSC.WMQ_HOST_NAME, (String)properties[XMSC.WMQ_HOST_NAME]);
            cf.SetIntProperty(XMSC.WMQ_PORT, Convert.ToInt32(properties[XMSC.WMQ_PORT]));
            cf.SetStringProperty(XMSC.WMQ_CHANNEL, (String)properties[XMSC.WMQ_CHANNEL]);
            cf.SetIntProperty(XMSC.WMQ_CONNECTION_MODE, XMSC.WMQ_CM_CLIENT);
            cf.SetStringProperty(XMSC.WMQ_QUEUE_MANAGER, (String)properties[XMSC.WMQ_QUEUE_MANAGER]);
            var shareCnv = Convert.ToBoolean(properties["ShareCnv"]);
            if (!shareCnv)
            {
                cf.SetIntProperty(XMSC.WMQ_SHARE_CONV_ALLOWED, XMSC.WMQ_SHARE_CONV_ALLOWED_NO);
            }
            return cf.CreateConnection();
        }

        void OnMessageCallback(IMessage message)
        {
            try
            {
                numberOfMsgs++;
            }
            catch (Exception ex)
            {
                Console.WriteLine("Exception caught in OnMessageCallback: {0}", ex);
                Environment.Exit(1);
            }
            if (numberOfMsgs == 500)
                timer.Start();
            if (numberOfMsgs == 9490)
            {
                sessionWMQ.Commit();
                timer.Stop();
                Console.WriteLine("Message Consumption Rate = Total Time taken to consume 10000 messages  = " + 10000 / timer.Elapsed.TotalSeconds + " messages/second");
                Console.WriteLine("Press any key!");
                complete = true;
            }
            sessionWMQ.Commit();
        }

        public void CreateThread()
        {
            try
            {
                int numberOfThreads = Convert.ToInt32(properties["NoofThreads"]);
                numberOfMsgs = Convert.ToInt32(properties["NoofMessages"]);
                var msgSize = Convert.ToInt32(properties["MessageSize"]);

                Task[] task = new Task[numberOfThreads];
                connectionWMQ = Connection();
                Console.WriteLine("-----------------------------------------------------");
                Console.WriteLine("PERFORMANCE STATISTICS");
                Console.WriteLine("-----------------------------------------------------");
                Console.WriteLine("Creating Threads:                         " + numberOfThreads);
                Console.WriteLine("Number of Messages:                       " + numberOfMsgs);
                Console.WriteLine("Message Size:                             " + msgSize);
                Console.WriteLine("-----------------------------------------------------");
                Console.WriteLine("Waiting to get statistics... ");
                for (int i = 0; i < numberOfThreads; ++i)
                {
                    task[i] = Task.Factory.StartNew(() => Publish());
                }
                Task.WaitAll(task);
                Console.WriteLine("-----------------------------------------------------");
                Console.WriteLine("Summary:");

                var max = myarray[0];
                for (int j = 0; j < numberOfThreads; ++j)
                {
                    if (myarray[j] > max)
                    {
                        max = myarray[j];
                    }
                }
                Console.WriteLine("Transfer Rate = Total Number of Messages/ Maximum Time taken  = " + numberOfMsgs + "/" + max + " = " + numberOfMsgs / max + " messages/second ");
                Console.WriteLine("-----------------------------------------------------");
            }
            catch (Exception ex)
            {
                Console.WriteLine("Exception caught: {0}", ex);
                Console.WriteLine("Sample execution  FAILED!");
            }
        }

        public bool Config()
        {
            try
            {
                // set the properties
                properties.Add(XMSC.WMQ_HOST_NAME, "localhost");
                properties.Add(XMSC.WMQ_PORT, 1414);
                properties.Add(XMSC.WMQ_CHANNEL,"SYSTEM.DEF.SVRCONN");
                properties.Add(XMSC.WMQ_QUEUE_MANAGER, "queueName");
                properties.Add(XMSC.WMQ_QUEUE_NAME, "queueManagerName");
                properties.Add("ShareCnv", false);
                properties.Add("MessageSize", 256);
                properties.Add("Persistence", 0);
                properties.Add("NoofThreads", 10);
                properties.Add("NoofMessages", 1000);
                return true;
            }
            catch(ArgumentException ex)
            {
                Console.WriteLine("Invalid arguments!\n{0}", ex);
                return false;
            }
            catch (Exception e)
            {
                Console.WriteLine("Exception caught while parsing command line arguments: " + e.Message);
                Console.WriteLine(e.StackTrace);
                return false;
            }
        }

    }
}
