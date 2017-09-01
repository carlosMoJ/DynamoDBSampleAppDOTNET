using Amazon.DynamoDBv2;
using Amazon.DynamoDBv2.DocumentModel;
using Amazon.DynamoDBv2.Model;
using DynamoWebApp.Models;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Web;

namespace DynamoWebApp.Classes
{
    public class DynamoAccess
    {
        public static AmazonDynamoDBClient GetLocalClient()
        {
            // First, set up a DynamoDB client for DynamoDB Local
            AmazonDynamoDBConfig ddbConfig = new AmazonDynamoDBConfig();
            ddbConfig.ServiceURL = "http://localhost:8000";
            AmazonDynamoDBClient client;
            try
            {
                client = new AmazonDynamoDBClient(ddbConfig);
            }
            catch (Exception ex)
            {
                Console.WriteLine("\n Error: failed to create a DynamoDB client; " + ex.Message);
                return (null);
            }
            return (client);
        }


        public static Table GetTableObject(AmazonDynamoDBClient client, string tableName)
        {
            Table table = null;
            try
            {
                table = Table.LoadTable(client, tableName);
            }
            catch (Exception ex)
            {
                Console.WriteLine("\n Error: failed to load the 'Movies' table; " + ex.Message);
                return (null);
            }
            return (table);
        }

        public Home GetData()
        {
            Home h = new Home();

            AmazonDynamoDBClient client = GetLocalClient();

            // Get a Table object for the table that you created in Step 1
            Table table = GetTableObject(client, "Movies");


            /*-----------------------------------------------------------------------
             *  4.2a:  Call Table.Scan to return the movies released in the 1950's,
             *         displaying title, year, lead actor and lead director.
             *-----------------------------------------------------------------------*/
            ScanFilter filter = new ScanFilter();
            filter.AddCondition("year", ScanOperator.Between, new DynamoDBEntry[] { 1950, 1959 });
            ScanOperationConfig config = new ScanOperationConfig
            {
                AttributesToGet = new List<string> { "year, title, info" },
                Filter = filter
            };
            Search search = table.Scan(filter);

            // Display the movie information returned by this query
            Console.WriteLine("\n\n Movies released in the 1950's (Document Model):" +
                       "\n--------------------------------------------------");
            h.info += "<br/><br/><b>Movies released in the 1950's (Document Model)</b><br/><br<br/>";
            List<Document> docList = new List<Document>();
            Document infoDoc;
            string movieFormatString = "    \"{0}\" ({1})-- lead actor: {2}, lead director: {3}";
            do
            {
                try
                {
                    docList = search.GetNextSet();
                }
                catch (Exception ex)
                {
                    Console.WriteLine("\n Error: Search.GetNextStep failed because: " + ex.Message);
                    break;
                }
                foreach (var doc in docList)
                {
                    infoDoc = doc["info"].AsDocument();
                    Console.WriteLine(movieFormatString,
                               doc["title"],
                               doc["year"],
                               infoDoc["actors"].AsArrayOfString()[0],
                               infoDoc["directors"].AsArrayOfString()[0]);
                    h.info += String.Format(movieFormatString,
                               doc["title"],
                               doc["year"],
                               infoDoc["actors"].AsArrayOfString()[0],
                               infoDoc["directors"].AsArrayOfString()[0]);
                    h.info += "</br>";
                }
            } while (!search.IsDone);

            /*-----------------------------------------------------------------------
             *  4.2b:  Call AmazonDynamoDBClient.Scan to return all movies released
             *         in the 1960's, only downloading the title, year, lead
             *         actor and lead director attributes.
             *-----------------------------------------------------------------------*/
            ScanRequest sRequest = new ScanRequest
            {
                TableName = "Movies",
                ExpressionAttributeNames = new Dictionary<string, string>
            {
                { "#yr", "year" }
            },
                ExpressionAttributeValues = new Dictionary<string, AttributeValue>
            {
                { ":y_a", new AttributeValue {
                      N = "1960"
                  } },
                { ":y_z", new AttributeValue {
                      N = "1969"
                  } },
            },
                FilterExpression = "#yr between :y_a and :y_z",
                ProjectionExpression = "#yr, title, info.actors[0], info.directors[0]"
            };

            ScanResponse sResponse = new ScanResponse();
            try
            {
                sResponse = client.Scan(sRequest);
            }
            catch (Exception ex)
            {
                Console.WriteLine("\n Error: Low-level scan failed, because: " + ex.Message);
            }

            // Display the movie information returned by this scan
            Console.WriteLine("\n\n Movies released in the 1960's (low-level):" +
                       "\n-------------------------------------------");
            h.info += "<br/><br/><b>Movies released in the 1960's (low-level)</b><br/><br/>";
            foreach (Dictionary<string, AttributeValue> item in sResponse.Items)
            {
                Dictionary<string, AttributeValue> info = item["info"].M;
                Console.WriteLine(movieFormatString,
                           item["title"].S,
                           item["year"].N,
                           info["actors"].L[0].S,
                           info["directors"].L[0].S);
                h.info += String.Format(movieFormatString,
                           item["title"].S,
                           item["year"].N,
                           info["actors"].L[0].S,
                           info["directors"].L[0].S);
                h.info += "</br>";
            }


            return h;
        }
    }
}