using System;

namespace MongoImportTool
{
    class Program
    {
        static void Main(string[] args)
        {
            ImportTool3 importTool3 = new ImportTool3();
            importTool3.Start(0);

            ////string srcCollection = "T_FacilityOperationRecord";
            //string srcUrl = "mongodb://sa:eagle-sight@mlk.mongodb.cheyunxin.com:27017/device";
            //string srcDb = "device";
            ////string dstCollection = "T_FacilityOperationRecord";
            //string dstUrl = "mongodb://eagle:Uvd7799nJ@192.168.100.221:27017/admin";
            //string dstDb = "device";

            //if (args.Length < 1)
            //{
            //    throw new Exception("参数异常！未指定迁移集合！");
            //}
            //string collection = args[0];

            //var start = 0;
            //if (args.Length == 2)
            //{
            //    if (!int.TryParse(args[1], out start))
            //    {
            //        throw new Exception("参数异常！起始位置描述不正常！");
            //    };
            //}

            //ImportTool2 tool2 = new ImportTool2(srcUrl, srcDb, collection, dstUrl, dstDb, collection);
            //tool2.Start(start);

            while (true)
            {
                var input = Console.ReadLine();
                if (input == "quit")
                {
                    break;
                }
            }
        }
    }
}
