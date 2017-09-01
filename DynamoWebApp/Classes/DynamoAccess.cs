using Amazon;
using Amazon.DynamoDBv2;
using Amazon.DynamoDBv2.DocumentModel;
using Amazon.DynamoDBv2.Model;
using DynamoWebApp.Models;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using System;
using System.Collections.Generic;
using System.IO;
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
        public static AmazonDynamoDBClient GetRemoteClient()
        {
            AmazonDynamoDBConfig ddbConfig = new AmazonDynamoDBConfig();
            // This client will access the US East 1 region.
            ddbConfig.RegionEndpoint = RegionEndpoint.EUWest2;
            AmazonDynamoDBClient client = new AmazonDynamoDBClient(ddbConfig);

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

        public Home GetData(bool local = true)
        {
            Home h = new Home();
            AmazonDynamoDBClient client;
            if (local)
            {
                client = GetLocalClient();
            }
            else
            {
                client = GetRemoteClient();
            }

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

        public Home CreateRemoteTable()
        {
            Home h = new Home();

            AmazonDynamoDBClient client = GetRemoteClient();

            // Build a 'CreateTableRequest' for the new table
            CreateTableRequest createRequest = new CreateTableRequest
            {
                TableName = "Movies",
                AttributeDefinitions = new List<AttributeDefinition>()
            {
                new AttributeDefinition
                {
                    AttributeName = "year",
                    AttributeType = "N"
                },
                new AttributeDefinition
                {
                    AttributeName = "title",
                    AttributeType = "S"
                }
            },
                KeySchema = new List<KeySchemaElement>()
            {
                new KeySchemaElement
                {
                    AttributeName = "year",
                    KeyType = "HASH"
                },
                new KeySchemaElement
                {
                    AttributeName = "title",
                    KeyType = "RANGE"
                }
            },
            };

            // Provisioned-throughput settings are required even though
            // the local test version of DynamoDB ignores them
            createRequest.ProvisionedThroughput = new ProvisionedThroughput(1, 1);

            // Using the DynamoDB client, make a synchronous CreateTable request
            CreateTableResponse createResponse;
            try
            {
                createResponse = client.CreateTable(createRequest);
            }
            catch (Exception ex)
            {
                Console.WriteLine("\n Error: failed to create the new table; " + ex.Message);
                h.info = "Error: failed to create the new table. " + ex.Message;
            }

            // Report the status of the new table...
            h.info = "Created the \"Movies\" table successfully";
            return h;
        }

        public static Table GetRemoteTableObject(string tableName)
        {
            // First, set up a DynamoDB client for DynamoDB Remote
            AmazonDynamoDBClient client = GetRemoteClient();

            // Now, create a Table object for the specified table
            Table table;
            try
            {
                table = Table.LoadTable(client, tableName);
            }
            catch (Exception ex)
            {
                return (null);
            }
            return (table);
        }

        public Home LoadSampleData()
        {
            Home h = new Home();
            StreamReader sr = null;
            JsonTextReader jtr = null;
            JArray movieArray = null;
            try
            {
                sr = new StreamReader("C:\\VS-Projects\\DynamoDBSampleApp\\DynamoWebApp\\moviedata.json");
                jtr = new JsonTextReader(sr);
                movieArray = (JArray)JToken.ReadFrom(jtr);
            }
            catch (Exception ex)
            {
                h.info = "Error: could not read from the 'moviedata.json' file, because: " + ex.Message;
            }
            finally
            {
                if (jtr != null)
                    jtr.Close();
                if (sr != null)
                    sr.Close();
            }

            // Get a Table object for the table that you created in Step 1
            Table table = GetRemoteTableObject("Movies");
            if (table == null)
            {
                h.info = "Didn't find table";
                return h;
            }

            // Load the movie data into the table (this could take some time)
            h.info += String.Format("Now writing {0:#,##0} movie records from moviedata.json (might take 15 minutes)...\n   ...completed: ", movieArray.Count);
            for (int i = 0, j = 99; i < movieArray.Count; i++)
            {
                try
                {
                    string itemJson = movieArray[i].ToString();
                    Document doc = Document.FromJson(itemJson);
                    table.PutItem(doc);
                }
                catch (Exception ex)
                {
                    h.info +=String.Format("Error: Could not write the movie record #{0:#,##0}, because {1}", i, ex.Message);
                }
                if (i >= j)
                {
                    j++;
                    h.info += String.Format("{0,5:#,##0}, ", j);
                    if (j % 1000 == 0)
                        h.info += String.Format("\n                 ");
                    j += 99;
                }
            }
            h.info += String.Format("\n   Finished writing all movie records to DynamoDB!");

            return h;
        }

        public Home AddNewEntry()
        {
            Home h = new Home();
            AmazonDynamoDBClient client = GetRemoteClient();
            Table table = GetTableObject(client,"Movies");
            if (table == null)
            {
                h.info = "Table not found";
                return h;
            }

            // Create a Document representing the movie item to be written to the table
            Document document = new Document();
            document["year"] = 2014;
            document["title"] = "The Big New Movie PREQUEL";
            document["century"] = 21;
            document["UploadedBy"] = "Carlos";
            document["info"] = Document.FromJson("{\"plot\" : \"Nothing happens at all.\",\"rating\" : 1,\"carlosrating\" : 2}");

            // Use Table.PutItem to write the document item to the table
            try
            {
                table.PutItem(document);
                h.info = "\nPutItem succeeded.\n";
            }
            catch (Exception ex)
            {
                h.info = "\n Error: Table.PutItem failed because: " + ex.Message;
            }

            return h;
        }
    }
}