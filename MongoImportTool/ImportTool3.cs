﻿using MongoDB.Bson;
using MongoDB.Driver;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace MongoImportTool
{
    public class ImportTool3
    {
        Stopwatch watch = new Stopwatch();
        const int READ_BATCH_SIZE = 102400;
        const int WRITE_BATCH_SIZE = 5120;

        string srcCollection = "T_CloudMirrorFacilityLocation";
        string srcUrl = "mongodb://eagle:Uvd7799nJ@192.168.100.221:27017/device";
        string srcDb = "device";


        string dstCollection = "T_CloudMirrorFacilityLocation";
        string dstUrl = "mongodb://eagle:Uvd7799nJ@192.168.100.11:27017/device_temp";
        string dstDb = "device_temp";

        public void Start(int start = 0)
        {
            var srcClient = new MongoClient(this.srcUrl);
            var dstClient = new MongoClient(this.dstUrl);

            var sdb = srcClient.GetDatabase(this.srcDb);
            var ddb = dstClient.GetDatabase(this.dstDb);

            Task.Run(async () =>
            {
                try
                {

                    watch.Start();

                    this.WriteConsole($"开始导入数据！起始位置：{start}！！");


                    var skip = start;
                    var totlewritecount = 0;
                    var totleskipcount = start;
                    var totlereadcount = start;
                    var builder = Builders<BsonDocument>.Filter;
                    FilterDefinition<BsonDocument> filter = builder.And(builder.Gte("CreateDate", new DateTime(2019, 3, 1)), builder.Lte("CreateDate", new DateTime(2019, 4, 1)));

                    //var cursor =await  sdb.GetCollection<BsonDocument>(this.srcCollection).FindAsync(new BsonDocument());

                    // var cursor = await sdb.GetCollection<BsonDocument>(this.srcCollection).Find(new BsonDocument()).Skip(skip).ToCursorAsync();
                    var cursor = await sdb.GetCollection<BsonDocument>(this.srcCollection).Find(filter).Skip(skip).ToCursorAsync();

                    //for (int i = 0; i < 73830400; i++)
                    //{
                    //    cursor.MoveNext();
                    //    this.WriteConsole(i.ToString());
                    //}



                    while (true)
                    {
                        List<BsonDocument> srcdocs = new List<BsonDocument>();
                        var index = skip;
                        //srcdocs.Clear();
                        while (cursor.MoveNext())
                        {
                            var temp = cursor.Current.ToList();

                            this.WriteConsole($"已从游标处获取{temp.Count}条记录！");
                            srcdocs.AddRange(temp);
                            //this.WriteConsole(srcdocs.Count.ToString());
                            if (srcdocs.Count >= READ_BATCH_SIZE)
                            {
                                break;
                            }
                        }
                        //for (int i = 0; i < READ_BATCH_SIZE; i++)
                        //{
                        //    srcdocs.AddRange(cursor.Current);
                        //    this.WriteConsole(i.ToString());
                        //    cursor.MoveNext();
                        //}
                        //var srcdocs = cursor.(skip).Limit(READ_BATCH_SIZE).ToList();
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
                            var t = Task.Run(() =>
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

                }
                catch (Exception ex)
                {
                    this.WriteConsole(ex.Message);

                }
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
