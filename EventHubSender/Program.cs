// Author: Galiya Warrier, Microsoft, 13/07/2017
// * * * ONLY TO USE FOR POC / DEV ENVIRONMENTS * * *

using Microsoft.Azure.EventHubs;
using System;
using System.IO;
using System.Text;
using System.Threading.Tasks;
using System.Configuration;
using Microsoft.WindowsAzure.Storage; 
using Microsoft.WindowsAzure.Storage.Blob; 

namespace EventHubSender
{
    class Program
    {
        //A client to interact with Azure Event Hub
        private static EventHubClient eventHubClient;
        
        //A message counter
        private static int numMessagesToSend = 0;

        public static void Main(string[] args)
        {
            MainAsync(args).GetAwaiter().GetResult();
        }

        private static async Task MainAsync(string[] args)
        {
            var appSettings = ConfigurationManager.AppSettings;

            //Collect Event Hub namespace configuration information
            var EhConnectionString = appSettings["EventHubNamespaceConnectionString"];
            var EhEntityPath = appSettings["EventHubName"];

            var connectionStringBuilder = new EventHubsConnectionStringBuilder(EhConnectionString)
            {
                EntityPath = EhEntityPath
            };

            // Connect Event Hub client
            eventHubClient = EventHubClient.CreateFromConnectionString(connectionStringBuilder.ToString());

            // Retrieve storage account from connection string.
            CloudStorageAccount storageAccount = CloudStorageAccount.Parse(appSettings["StorageConnectionString"]);

            // Create the blob client.
            CloudBlobClient blobClient = storageAccount.CreateCloudBlobClient();

            // Retrieve reference to a previously created container.
            CloudBlobContainer container = blobClient.GetContainerReference(appSettings["BlobContainerName"]);

            // Retrieve blob directory
            string blobDirectory = null;
            if (args.Length > 0 && args[0].Contains("dir"))
            {
                blobDirectory = appSettings["BlobDirectory"];
            }

            // Iterate through each file in the blob directory
            try
            {
                foreach (IListBlobItem item in container.ListBlobs(blobDirectory, true))
                {
                    if (item.GetType() == typeof(CloudBlockBlob))
                    {
                        // Read a blob file
                        CloudBlockBlob blob = (CloudBlockBlob)item;
                        Console.WriteLine($"Sending contents of {item.Uri} file to EH.");

                        // Read blob file as a stream line-by-line
                        using (StreamReader sr = new StreamReader(blob.OpenRead()))
                        {
                            while (sr.Peek() >= 0)
                            {
                                var singleLine = sr.ReadLine();

                                // Send json to Event Hub as a separate message
                                // TODO: Can be improved to send batch events (https://docs.microsoft.com/en-us/rest/api/eventhub/send-batch-events)
                                await SendMessagesToEventHub(singleLine);

                                if ((numMessagesToSend % 1000) == 0)
                                {
                                    Console.WriteLine($"{numMessagesToSend} messages sent.");
                                }

                            }
                        }

                    }
                }
            }

            catch (Exception e)
            {
                Console.WriteLine($"Exception: {e.InnerException}");
                Console.ReadLine();
            }
            await eventHubClient.CloseAsync();

            Console.WriteLine($"{numMessagesToSend} messages sent.");
            Console.WriteLine("Press ENTER to exit.");
            Console.ReadLine();
        }

        
        private static async Task SendMessagesToEventHub(string jsonMessage)
        {
            // Send a single json message to the Event Hub
            try
            {
                await eventHubClient.SendAsync(new EventData(Encoding.UTF8.GetBytes(jsonMessage)));
            }
            catch (Exception exception)
            {
                Console.WriteLine($"{DateTime.Now} > Exception: {exception.Message}");
            }
            numMessagesToSend++;
        }
    }
}