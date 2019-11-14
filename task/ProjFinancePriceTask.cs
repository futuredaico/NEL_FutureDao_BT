using NEL.NNS.lib;
using NEL_FutureDao_BT.core;
using NEL_FutureDao_BT.lib;
using NEL_FutureDao_BT.task.dao;
using Newtonsoft.Json.Linq;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;

namespace NEL_FutureDao_BT.task
{
    class ProjFinancePriceTask : AbsTask
    {
        private MongoDBHelper mh = new MongoDBHelper();
        private DbConnInfo daoConn;
        private string notifyCounter = "daonotifycounters";
        private string notifyCol = "daonotifyinfos";
        private string projFinanceHashCol = "daoprojfinancehashinfos";
        //
        private string projFinancePriceHistCol = "daoprojfinancepricehistinfos";
        private DbConnInfo blockConn;
        private long OneHourSeconds = 3600;

        public ProjFinancePriceTask(string name) : base(name)
        {

        }
        public override void initConfig(JObject config)
        {
            daoConn = Config.daoDbConnInfo;
            blockConn = Config.remoteDbConnInfo;
        }

        public override void process()
        {
            Thread.Sleep(1000*10);
            handleTask();
            Console.WriteLine("{0} is running", name());
        }
        private void handleTask()
        {
            string key = "price";
            long nt = nowTime;
            long lt = getL(key);
            if (lt == -1)
            {
                lt = getFirstBlockTime();
            }
            if (lt == -1)
            {
                return;
            }
            if (nt < lt + OneHourSeconds)
            {
                return;
            }
            long rt = getR();
            if (rt < lt + OneHourSeconds)
            {
                return;
            }

            var projIds = getProjId();
            if (projIds == null) return;

            long tm = lt + OneHourSeconds;
            foreach (var projId in projIds)
            {
                var findStr = new JObject { { "projId", projId }, { "recordTime", tm } }.ToString();
                if (mh.GetDataCount(daoConn.connStr, daoConn.connDB, projFinancePriceHistCol, findStr) > 0)
                {
                    continue;
                }
                //
                bool flag = false;
                var newdata = new TokenPriceInfo();
                var res = getLastBuySellPrice(projId, "OnBuy", tm);
                if (res != null)
                {
                    newdata.ob_address = res["who"].ToString();
                    newdata.ob_fundAmt = decimal.Parse(res["fundAmt"].ToString()).format();
                    newdata.ob_tokenAmt = decimal.Parse(res["tokenAmt"].ToString()).format();
                    newdata.ob_price = (newdata.ob_fundAmt.format() / newdata.ob_tokenAmt.format()).formatPrecision().format();
                    newdata.ob_txid = res["transactionHash"].ToString();
                    newdata.ob_blockTime = long.Parse(res["blockTime"].ToString());
                    newdata.contractHash = res["contractHash"].ToString();
                    newdata.projId = res["projId"].ToString();
                    flag = true;
                }

                res = getLastBuySellPrice(projId, "OnSell", tm);
                if (res != null)
                {
                    newdata.os_address = res["who"].ToString();
                    newdata.os_fundAmt = decimal.Parse(res["fundAmt"].ToString()).format();
                    newdata.os_tokenAmt = decimal.Parse(res["tokenAmt"].ToString()).format();
                    newdata.os_price = (newdata.os_fundAmt.format()/ newdata.os_tokenAmt.format()).formatPrecision().format();
                    newdata.os_txid = res["transactionHash"].ToString();
                    newdata.os_blockTime = long.Parse(res["blockTime"].ToString());
                    newdata.contractHash = res["contractHash"].ToString();
                    newdata.projId = res["projId"].ToString();
                    flag = true;
                }
                if (flag)
                {
                    newdata.recordType = getRecordType();
                    newdata.recordTime = tm;
                    mh.PutData<TokenPriceInfo>(daoConn.connStr, daoConn.connDB, projFinancePriceHistCol, newdata);
                }
            }
            updateL(key, tm);
            Console.WriteLine("{0}.[{1}] has processed:{2}", name(), key, tm);
        }

        private int getRecordType()
        {
            return DateTime.Now.Hour/4 == 0 ? RecordType.Four: RecordType.One;
        }

        private string[] getProjId()
        {
            var list = new List<string>();
            list.Add(new JObject { { "$group", new JObject { { "_id", "$projId" }, { "sum", new JObject { { "$sum", 1 } } } } } }.ToString());
            var res = mh.Aggregate(daoConn.connStr, daoConn.connDB, projFinanceHashCol, list);
            if (res.Count == 0) return null;

            return res.Select(p => p["_id"].ToString()).ToArray();
        }

        private JToken getLastBuySellPrice(string projId, string eventName, long time)
        {
            var findStr = new JObject { { "projId", projId}, { "event", eventName }, { "blockTime", new JObject { { "$lte", time } } } }.ToString();
            var sortStr = new JObject { { "blockNumber", -1 } }.ToString();
            var queryRes = mh.GetData(daoConn.connStr, daoConn.connDB, notifyCol, findStr, sortStr, 0, 1);
            if (queryRes.Count == 0) return null;
            return queryRes[0];
        }

        private long nowTime => TimeHelper.GetTimeStamp();
       
        private long getFirstBlockTime()
        {
            var findStr = "{}";
            var sortStr = new JObject { { "blockNumber", 1 } }.ToString();
            var queryRes = mh.GetData(daoConn.connStr, daoConn.connDB, notifyCol, findStr, sortStr, 0, 1);
            if (queryRes.Count == 0) return -1;
            var blockTime = long.Parse(queryRes[0]["blockTime"].ToString());
            //
            var date = TimeHelper.transDateTime(blockTime);
            date = date.AddMinutes(date.Minute*-1);
            date = date.AddSeconds(date.Second*-1);
            return TimeHelper.transTimeStamp(date);
        }

        private void updateL(string key, long tm)
        {
            string findStr = new JObject { { "counter", key } }.ToString();
            if(mh.GetDataCount(daoConn.connStr, daoConn.connDB, notifyCounter, findStr) == 0)
            {
                var newdata = new JObject { { "counter", key }, { "lastBlockIndex", tm } }.ToString();
                mh.PutData(daoConn.connStr, daoConn.connDB, notifyCounter, newdata);
                return;
            }
            var updateStr = new JObject { { "$set", new JObject { { "lastBlockIndex", tm } } } }.ToString();
            mh.UpdateData(daoConn.connStr, daoConn.connDB, notifyCounter, updateStr, findStr);
        }
        private long getL(string key)
        {
            string findStr = new JObject { { "counter", key } }.ToString();
            var queryRes = mh.GetData(daoConn.connStr, daoConn.connDB, notifyCounter, findStr);
            if (queryRes.Count == 0) return -1;
            return long.Parse(queryRes[0]["lastBlockIndex"].ToString());
        }
        private long getR(string key = "logs")
        {
            return GetRBlockTime();
        }

        private long GetRBlockTime()
        {
            // 最新时间
            return getBlockTime(GetRBlockIndex());
        }
        private long GetRBlockIndex()
        {
            var findStr = new JObject { { "counter", "logs" } }.ToString();
            var queryRes = mh.GetData(daoConn.connStr, daoConn.connDB, notifyCounter, findStr);
            if (queryRes.Count == 0) return -1;
            return long.Parse(queryRes[0]["lastBlockIndex"].ToString());
        }
        private long getBlockTime(long blockNumber)
        {
            var findStr = new JObject { {"number", blockNumber } }.ToString();
            var fieldStr = new JObject { { "timestamp", 1 } }.ToString();
            var queryRes = mh.GetData(blockConn.connStr, blockConn.connDB, "blocks", findStr, fieldStr);
            if (queryRes.Count == 0) return -1;
            return long.Parse(queryRes[0]["timestamp"].ToString());

        }
    }
    class RecordType
    {
        public const int One = 1;
        public const int Four = 4;
    }
}
