using MongoDB.Bson;
using MongoDB.Driver;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Text;
using System.Threading.Tasks;
using System.Linq;
using System.IO;

namespace MongoImportTool
{
    public class ImportTool
    {
        Stopwatch watch = new Stopwatch();
        const int READ_BATCH_SIZE = 102400;
        const int WRITE_BATCH_SIZE = 5120;

        string srcCollection = "T_FacilityOperationRecord";
        string srcUrl = "mongodb://sa:eagle-sight@mlk.mongodb.cheyunxin.com:27017/device";
        string srcDb = "device";
        string dstCollection = "T_FacilityOperationRecord";
        string dstUrl = "mongodb://eagle:Uvd7799nJ@192.168.100.221:27017/admin";
        string dstDb = "device";
        public ImportTool(string srcUrl, string srcDb, string srcCollection, string dstUrl, string dstDb, string dstCollection)
        {
            this.srcUrl = srcUrl;
            this.srcDb = srcDb;
            this.srcCollection = srcCollection;
            this.dstUrl = dstUrl;
            this.dstDb = dstDb;
            this.dstCollection = dstCollection;
        }

        public void Start(int start = 0)
        {
            var srcClient = new MongoClient(this.srcUrl);
            var dstClient = new MongoClient(this.dstUrl);

            var sdb = srcClient.GetDatabase(this.srcDb);
            var ddb = dstClient.GetDatabase(this.dstDb);

            Task.Run(() =>
            {

                watch.Start();
                this.WriteConsole();
                this.WriteConsole();
                this.WriteConsole();
                this.WriteConsole();
                this.WriteConsole($"开始导入数据！起始位置：{start}！！");


                var skip = start;
                var totlewritecount = 0;
                var totleskipcount = start;
                var totlereadcount = start;

                while (true)
                {
                    var index = skip;
                    var srcdocs = sdb.GetCollection<BsonDocument>(this.srcCollection).Find(new BsonDocument()).Skip(skip).Limit(READ_BATCH_SIZE).ToList();
                    lock (this)
                    {
                        skip += srcdocs.Count;
                    }
                    if (srcdocs.Count == 0)
                    {
                        this.WriteConsole($"所有数据已读取完毕！！！");
                        break;
                    }
                    this.WriteConsole($"已读取{srcdocs.Count}条记录！");
                    lock (this)
                    {
                        totlereadcount += srcdocs.Count;
                    }

                    var wskip = 0;
                    while (true)
                    {
                        var windex = wskip;
                        var newdocs = srcdocs.Skip(wskip).Take(WRITE_BATCH_SIZE).ToList();
                        lock (this)
                        {
                            wskip += newdocs.Count;
                        }
                        if (newdocs.Count == 0)
                        {
                            break;
                        }
                        Task.Run(() =>
                        {

                            try
                            {
                                ddb.GetCollection<BsonDocument>(this.dstCollection).InsertMany(newdocs, new InsertManyOptions() { IsOrdered = false });
                                lock (this)
                                {
                                    totlewritecount += newdocs.Count;
                                }
                            }
                            catch (MongoBulkWriteException<BsonDocument> ex)
                            {
                                //this.WriteConsole(ex.Message);
                                lock (this)
                                {
                                    totlewritecount += (int)(ex.Result.ModifiedCount + ex.Result.InsertedCount);
                                    totleskipcount += ex.WriteErrors.Count;
                                }
                            }
                            this.WriteConsole($"({index + windex},{newdocs.Count})已跳过：{totleskipcount}，已写入：{totlewritecount}，已读取：{totlereadcount}");

                        });

                    }
                }

                //watch.Stop();
            });

        }


        public void WriteConsole(string mes = null)
        {
            var time = watch.Elapsed;
            try
            {
                lock (this)
                {
                    Console.WriteLine($"[{time}]{mes}");
                    File.AppendAllLines("log.log", new[] { $"[{time}]{mes}" });
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine($"{ex.ToString()}:{ex.Message}\r\n{ex.StackTrace}");
            }
        }


    }
}
