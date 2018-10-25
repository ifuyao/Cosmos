using System;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.Configuration;
using System.IO;
using System.Linq;
using System.Net;
using System.Net.Http;
using System.Threading.Tasks;
using System.Web;
using Microsoft.Azure.Documents;
using Microsoft.Azure.Documents.Client;
using Newtonsoft.Json;

namespace AutoChangeRU
{
    public class Program
    {
        //Azure订阅Id
        private static string Subscriptions { get; set; }

        //注册应用ClientId ,注意注册应用需要在对应Cosmos Accout 添加访问控制
        private static string CosmosRuSettingClientId { get; set; }

        //注册应用Secret
        private static string CosmosRuSettingClientSecret { get; set; }

        //AAD TenantId
        private static string TenantId { get; set; }

        static Program()
        {
            Subscriptions = ConfigurationManager.AppSettings["Subscription"];
            CosmosRuSettingClientId = ConfigurationManager.AppSettings["CosmosRuSettingClientId"];
            CosmosRuSettingClientSecret = ConfigurationManager.AppSettings["CosmosRuSettingClientSecret"];
            TenantId = ConfigurationManager.AppSettings["TenantId"];
        }

        protected static HttpClient NewHttpClient()
        {
            var handler = new HttpClientHandler();
            ServicePointManager.ServerCertificateValidationCallback += (sender, cert, chain, sslPolicyErrors) => true;
            ServicePointManager.DefaultConnectionLimit = 1024;
            return new HttpClient(handler);

        }

        static void Main(string[] args)
        {
            Execute();
            Console.ReadLine();
        }

        public static void Execute()
        {
            try
            {
                //读取需要调整Cosmos RU Collection的配置
                var cosmosInfoStr = File.ReadAllText("CosmosConfig.json");
                if (!string.IsNullOrEmpty(cosmosInfoStr))
                {
                    var cosmosInfos = JsonConvert.DeserializeObject<List<CosmosInfo>>(cosmosInfoStr);
                    var token = GetToken();
                    foreach (var cosmosInfo in cosmosInfos)
                    {
                        foreach (var database in cosmosInfo.DatabaseList)
                        {
                            foreach (var collection in database.CollectionList)
                            {
                                //获取分区数量
                                var pkr = GetPartitionKeyRanges(cosmosInfo.DatabaseAccount, cosmosInfo.PrimaryKey,
                                    database.Name, collection.Name);
                                if (pkr > 0)
                                {
                                    //获取分区近五分钟平均每秒的RU消耗值
                                    var rucounts = GetFiveMinuteMetricRuCount(token, cosmosInfo.DatabaseAccount,
                                        cosmosInfo.ResourceGroup, database.Key,
                                        collection.Key);
                                    if (rucounts != null && rucounts.Count > 1)
                                    {
                                        //近5分钟RU判平均数
                                        var avg = rucounts.Average();
                                        //调整Cosmos RU
                                        new Program().ChangeCosmosRu(cosmosInfo.DatabaseAccount, cosmosInfo.PrimaryKey,
                                            database.Name,
                                            collection, pkr, avg);
                                    }
                                    else
                                    {
                                        Console.WriteLine(database.Name + "-" + collection.Name + "，暂无监控数据，无需调整设置");
                                    }
                                }
                                else
                                {
                                    Console.WriteLine("无法获取分区数据");
                                }
                            }
                        }
                    }
                }
                else
                {
                    Console.WriteLine("无法获取配置文件");
                }
            }
            catch (Exception ex)
            {
                string sErrorMsg = ex.Message;
                if (ex.InnerException != null)
                {
                    sErrorMsg += ex.InnerException.Message;
                }
                Console.WriteLine("Execute-" + sErrorMsg);
            }
        }

        #region  Get Token
        public static string GetToken()
        {
            try
            {
                var url = $"https://login.chinacloudapi.cn/{TenantId}/oauth2/token?api-version=1.0";
                var formContent = new FormUrlEncodedContent(new[]
                {
                    new KeyValuePair<string, string>("grant_type", "client_credentials"),
                    new KeyValuePair<string, string>("resource", "https://management.core.chinacloudapi.cn/"),
                    new KeyValuePair<string, string>("client_id", CosmosRuSettingClientId),
                    new KeyValuePair<string, string>("client_secret", CosmosRuSettingClientSecret)
                });
                var httpClient = NewHttpClient();
                var result = httpClient.PostAsync(url, formContent).Result.Content.ReadAsStringAsync().Result;
                return JsonConvert.DeserializeObject<dynamic>(result).access_token;
            }
            catch (Exception ex)
            {
                string sErrorMsg = ex.Message;
                if (ex.InnerException != null)
                {
                    sErrorMsg += ex.InnerException.Message;
                }
                Console.WriteLine("GetToken-"+sErrorMsg);
                return string.Empty;
            }
        }

        #endregion

        #region 获取近5分钟的监控数据

        public static List<int> GetFiveMinuteMetricRuCount(string token, string databaseAccount, string resourceGroup, string database, string collection)
        {
            try
            {
                var url =
                    $"https://management.chinacloudapi.cn/subscriptions/{Subscriptions}/resourceGroups/{resourceGroup}/providers/Microsoft.DocumentDb/databaseAccounts/{databaseAccount}/databases/{database}/collections/{collection}/metrics?api-version=2015-04-08&$filter=";
                var startTime = DateTime.UtcNow.AddMinutes(-10).ToString("yyyy-MM-ddTHH:mm:00.0000000Z");
                var endTime = DateTime.UtcNow.AddMinutes(-5).ToString("yyyy-MM-ddTHH:mm:00.0000000Z");
                var filter =
                    $"(name.value eq 'Max RUs Per Second') and timeGrain eq duration'PT1M' and startTime eq {startTime} and endTime eq {endTime}";
                var httpClient = NewHttpClient();
                httpClient.DefaultRequestHeaders.Add("Authorization", "Bearer " + token);
                var result = httpClient.GetAsync(url + filter).Result.Content.ReadAsStringAsync().Result;
                if (result.Length > 20)
                {
                    var metrics = JsonConvert.DeserializeObject<Metrics>(result);
                    return metrics.value.LastOrDefault().metricValues.Select(item => item._count).ToList();
                }
                return null;
            }
            catch (Exception ex)
            {
                string sErrorMsg = ex.Message;
                if (ex.InnerException != null)
                {
                    sErrorMsg += ex.InnerException.Message;
                }
                Console.WriteLine("GetFiveMinuteMetricRUCount-"+sErrorMsg);
                return null;
            }
        }


        #endregion

        #region 调整Cosmos RU
        private async  void ChangeCosmosRu(string databaseAccount, string primaryKey, string databasename, Collection coll, int pkr, double averageRu)
        {
            try
            {
                var endPointUrl = $"https://{databaseAccount}.documents.azure.cn:443";
                var documentClient = new DocumentClient(new Uri(endPointUrl), primaryKey);
                Microsoft.Azure.Documents.Database database = await GetOrCreateDatabaseAsync(documentClient,
                    databasename);
                DocumentCollection collection = await GetOrCreateCollectionAsync(documentClient, database.SelfLink,
                    coll.Name);

                Offer offer = documentClient.CreateOfferQuery()
                    .Where(r => r.ResourceLink == collection.SelfLink)
                    .AsEnumerable()
                    .SingleOrDefault();
                int offerThroughput = JsonConvert.DeserializeObject<dynamic>(offer.ToString()).content.offerThroughput; //当前设置的RU
                var currentThroughput = pkr * averageRu; //当前实时RU
                var ruRate = Math.Round(currentThroughput / offerThroughput, 2);//计算实时RU和设置的RU占
                if ((ruRate - coll.IncreaseRate >= coll.ThresholdRate || ruRate + coll.IncreaseRate < coll.ThresholdRate) && currentThroughput > 1000)
                {
                    var newOfferThroughput = Convert.ToInt32(currentThroughput / coll.ThresholdRate); //保证RU为阈值左右
                    var increaseValue = Math.Abs(newOfferThroughput - currentThroughput); //计算调整RU的差值
                    if (increaseValue > 100)
                    {
                        newOfferThroughput = newOfferThroughput - newOfferThroughput % 100 + 100;
                        if (newOfferThroughput < coll.DefaultValue)
                        {
                            newOfferThroughput = coll.DefaultValue;
                        }
                        offer = new OfferV2(offer, newOfferThroughput);
                        Console.WriteLine(databasename + "-" + coll.Name + "，当前设置" + offerThroughput + "RU" + ",当前消耗" +
                                          currentThroughput + "RU" + ",调整设置为" + newOfferThroughput + "RU");
                        await documentClient.ReplaceOfferAsync(offer);
                    }
                    else
                    {

                        Console.WriteLine(databasename + "-" + coll.Name + "，当前设置" + offerThroughput + "RU" + ",当前消耗" +
                                          currentThroughput + "RU" + "，无需调整设置，调整幅度最小为100RU");
                    }
                }
                else
                {
                    Console.WriteLine(databasename + "-" + coll.Name + "，当前设置" + offerThroughput + "RU" + ",当前消耗" +
                                      currentThroughput + "RU" + ",无需调整设置");
                }
            }
            catch (Exception ex)
            {
                string sErrorMsg = ex.Message;
                if (ex.InnerException != null)
                {
                    sErrorMsg += ex.InnerException.Message;
                }
                Console.WriteLine("ChangeCosmosRU-"+sErrorMsg);
            }
        }


        private async Task<Microsoft.Azure.Documents.Database> GetOrCreateDatabaseAsync(DocumentClient documentClient, string id)
        {
            return documentClient.CreateDatabaseQuery().Where(db => db.Id == id).ToArray().FirstOrDefault() ??
                           await documentClient.CreateDatabaseAsync(new Microsoft.Azure.Documents.Database { Id = id });
        }

        private async Task<DocumentCollection> GetOrCreateCollectionAsync(DocumentClient documentClient, string dbLink, string id)
        {
            DocumentCollection collection = documentClient.CreateDocumentCollectionQuery(dbLink).Where(c => c.Id == id).ToArray().FirstOrDefault();
            if (collection == null)
            {
                collection = new DocumentCollection { Id = id };
                collection.IndexingPolicy.IncludedPaths.Add(new IncludedPath
                {
                    Path = "/*",
                    Indexes = new Collection<Index>(new Index[]
                    {
                        new RangeIndex(DataType.Number) { Precision = -1},
                        new RangeIndex(DataType.String) { Precision = -1},
                    }),
                });

                collection = await documentClient.CreateDocumentCollectionAsync(dbLink, collection);
            }
            return collection;
        }
        #endregion

        #region 获取分区数
        public static int GetPartitionKeyRanges(string databaseaccount, string key, string db, string coll)
        {
            try
            {
                var utcDate = DateTime.UtcNow.ToString("r");
                var token = GenerateMasterKeyAuthorizationSignature("GET",
                    $"dbs/{db}/colls/{coll}", "pkranges",
                   key, "master",
                    "1.0", utcDate);
                var url = $"https://{databaseaccount}.documents.azure.cn/dbs/{db}/colls/{coll}/pkranges";
                var httpClient = NewHttpClient();
                httpClient.DefaultRequestHeaders.Add("authorization", token);
                httpClient.DefaultRequestHeaders.Add("x-ms-version", "2016-07-11");
                httpClient.DefaultRequestHeaders.Add("x-ms-date", utcDate);
                var result = httpClient.GetAsync(url).Result.Content.ReadAsStringAsync().Result;
                return JsonConvert.DeserializeObject<dynamic>(result)._count;
            }
            catch (Exception ex)
            {
                string sErrorMsg = ex.Message;
                if (ex.InnerException != null)
                {
                    sErrorMsg += ex.InnerException.Message;
                }
                Console.WriteLine("GetPartitionKeyRanges-"+ sErrorMsg);
                return 0;
            }
        }

        public static string GenerateMasterKeyAuthorizationSignature(string verb, string resourceId, string resourceType, string key, string keyType, string tokenVersion, string utc_date)
        {
            var hmacSha256 = new System.Security.Cryptography.HMACSHA256 { Key = Convert.FromBase64String(key) };

            string payLoad = string.Format(System.Globalization.CultureInfo.InvariantCulture, "{0}\n{1}\n{2}\n{3}\n{4}\n",
                    verb.ToLowerInvariant(),
                    resourceType.ToLowerInvariant(),
                    resourceId,
                   utc_date.ToLowerInvariant(),
                    ""
            );

            var hashPayLoad = hmacSha256.ComputeHash(System.Text.Encoding.UTF8.GetBytes(payLoad));
            var signature = Convert.ToBase64String(hashPayLoad);

            return HttpUtility.UrlEncode(String.Format(System.Globalization.CultureInfo.InvariantCulture, "type={0}&ver={1}&sig={2}",
                keyType,
                tokenVersion,
                signature));
        }

        #endregion

        #region Models
        public class CosmosInfo
        {
            public string DatabaseAccount { get; set; }

            public string ResourceGroup { get; set; }

            public string PrimaryKey { get; set; }

            public List<Database> DatabaseList { get; set; }
        }

        public class Database
        {
            public string Key { get; set; }

            public string Name { get; set; }

            public List<Collection> CollectionList { get; set; }
        }

        public class Collection
        {
            public string Key { get; set; }

            public string Name { get; set; }

            public int DefaultValue { get; set; }

            public double ThresholdRate { get; set; }

            public double IncreaseRate { get; set; }
        }

        public class Metrics
        {
            public List<MetricsInfo> value { get; set; }
        }

        public class MetricsInfo
        {
            public string timeGrain { get; set; }

            public DateTime startTime { get; set; }

            public DateTime endTime { get; set; }

            public string unit { get; set; }

            public List<metricValue> metricValues { get; set; }
        }

        public class metricValue
        {
            public DateTime timestamp { get; set; }

            public int total { get; set; }

            public int _count { get; set; }

            public int maximum { get; set; }
        }
        #endregion
    }
}
