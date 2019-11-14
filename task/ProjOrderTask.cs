using NEL.NNS.lib;
using NEL_FutureDao_BT.core;
using NEL_FutureDao_BT.lib;
using Newtonsoft.Json.Linq;
using System;
using System.Linq;

namespace NEL_FutureDao_BT.task
{
    class ProjOrderTask : AbsTask
    {
        private MongoDBHelper mh = new MongoDBHelper();
        private DbConnInfo daoConn;
        private string notifyCounter = "daonotifycounters";
        private string notifyCol = "daonotifyinfos";
        private string projFinanceOrderCol = "daoprojfinanceorderinfos";
        private string projFinanceRewardCol = "daoprojfinancerewardinfos";
        private long OrderValidTime = 10 * 60;

        public ProjOrderTask(string name) : base(name) { }

        public override void initConfig(JObject config)
        {
            daoConn = Config.daoDbConnInfo;
        }

        public override void process()
        {
            Sleep(2000);
            processOrder();
            processOrderTimeout();
            processOrderCounter();
        }

        private void processOrder()
        {
            string key = "order";
            long lh = getL(key);
            long rh = getR();
            if (lh >= rh)
            {
                return;
            }
            for (var index = lh + 1; index <= rh; ++index)
            {
                var findStr = new JObject { { "blockNumber", index },{ "event", "OnEvent" } }.ToString();
                var queryRes = mh.GetData(daoConn.connStr, daoConn.connDB, notifyCol, findStr);
                if (queryRes.Count == 0)
                {
                    updateL(key, index);
                    log(key, index, rh);
                    continue;
                }
                foreach(var item in queryRes)
                {
                    handleOrder(item);
                }
                //
                updateL(key, index);
                log(key, index, rh);
            }
        }
        private void handleOrder(JToken jt)
        {
            string orderId = jt["orderId"].ToString();
            if (orderId == "0") return;

            var findStr = new JObject { { "orderId", orderId } }.ToString();
            var fieldStr = new JObject { { "txid", 1 }, { "orderState", 1 } }.ToString();
            var queryRes = mh.GetData(daoConn.connStr, daoConn.connDB, projFinanceOrderCol, findStr, fieldStr);
            if (queryRes.Count == 0) return;

            var item = queryRes[0];
            string orderState = item["orderState"].ToString();
            if ( orderState == OrderState.WaitingPay 
                    || orderState == OrderState.WaitingConfirm
                    || orderState == OrderState.Canceled)
            {
                var updateStr = new JObject { { "$set", new JObject { { "orderState", OrderState.WaitingDeliverGoods } } } }.ToString();
                mh.UpdateData(daoConn.connStr, daoConn.connDB, projFinanceOrderCol, updateStr, findStr);
            }
        }

        private void processOrderTimeout()
        {
            var now = TimeHelper.GetTimeStamp();
            var findStr = new JObject { {"orderState", OrderState.WaitingPay }, { "time", new JObject { { "$lt", now - OrderValidTime } } } }.ToString();
            var queryRes = mh.GetData(daoConn.connStr, daoConn.connDB, projFinanceOrderCol, findStr);
            if (queryRes.Count == 0) return;

            foreach(var item in queryRes)
            {
                handleOrderTimeout(item);
            }
        }
        private void handleOrderTimeout(JToken jt)
        {
            var findStr = new JObject { { "orderId", jt["orderId"] } }.ToString();
            var updateStr = new JObject { { "$set", new JObject { { "orderState", OrderState.PayTimeout } } } }.ToString();
            mh.UpdateData(daoConn.connStr, daoConn.connDB, projFinanceOrderCol, updateStr, findStr);
        }
        
        private void processOrderCounter()
        {
            var lastUpdateTime = queryLastUpdateTime();
            var now = TimeHelper.GetTimeStamp();
            var findStr = new JObject { { "markTime", new JObject { { "$lte", now},{ "$gt", lastUpdateTime} } } }.ToString();
            var fieldStr = new JObject { { "rewardId",1},{ "orderId", 1 }, { "orderState", 1 } }.ToString();
            var queryRes = mh.GetData(daoConn.connStr, daoConn.connDB, projFinanceOrderCol, findStr, fieldStr);
            if (queryRes.Count == 0) return;

            var res = 
            queryRes.GroupBy(p => p["rewardId"], (k, g) =>
            {
                var rewardId = k.ToString();
                var count = g.Sum(pg =>
                {
                    var sOrderState = pg["orderState"].ToString();
                    if (sOrderState == OrderState.Canceled
                        || sOrderState == OrderState.PayTimeout
                        || sOrderState == OrderState.TxFailed)
                    {
                        return -1;
                    }
                    return 1;
                });
                return new { _rewardId = rewardId, _count = count};
            }).ToArray();
            //
            foreach(var item in res)
            {
                handleOrderCounter(item._rewardId, item._count);
            }
            updateLastUpdateTime(now);
            //
            tempNotClearAllFlag = false;
            try
            {
                var ids = res.Select(p => p._rewardId).ToArray();
                clearTempData(ids);
            } catch(Exception ex)
            {
                Console.WriteLine(ex);
                tempNotClearAllFlag = true;
            }

        }
        private bool tempNotClearAllFlag = false;
        private void clearTempData(string[] rewardIds)
        {
            foreach(var rewardId in rewardIds)
            {
                var findStr = new JObject { { "rewardId", rewardId } }.ToString();
                var updateStr = new JObject { { "$set", new JObject { { "hasSellCountTp", 0 } } } }.ToString();
                mh.UpdateData(daoConn.connStr, daoConn.connDB, projFinanceRewardCol, updateStr, findStr);
            }
        }
        private void handleOrderCounter(string rewardId, int count)
        {
            var findStr = new JObject { { "rewardId", rewardId } }.ToString();
            var fieldStr = new JObject { { "hasSellCount", 1 }, { "hasSellCountTp", 1 } }.ToString();
            var queryRes = mh.GetData(daoConn.connStr, daoConn.connDB, projFinanceRewardCol, findStr, fieldStr);
            if (queryRes.Count == 0) return;

            var item = queryRes[0];
            var oldCount = long.Parse(item["hasSellCount"].ToString());
            var oldCountTp = long.Parse(item["hasSellCountTp"].ToString());
            if (tempNotClearAllFlag) oldCountTp = 0;

            oldCount += count - oldCountTp;
            oldCountTp = count;
            var updateStr = new JObject { { "$set", new JObject { { "hasSellCount", oldCount }, { "hasSellCountTp", oldCountTp } } } }.ToString();
            mh.UpdateData(daoConn.connStr, daoConn.connDB, projFinanceRewardCol, updateStr, findStr);
        }


        private void updateLastUpdateTime(long time)
        {
            var key = "orderMarkTime";
            var findStr = new JObject { { "counter", key } }.ToString();
            if (mh.GetDataCount(daoConn.connStr, daoConn.connDB, notifyCounter, findStr) == 0)
            {
                var newdata = new JObject { { "counter", key }, { "lastBlockTime", time } }.ToString();
                mh.PutData(daoConn.connStr, daoConn.connDB, notifyCounter, newdata);
                return;
            }
            var updateStr = new JObject { { "$set", new JObject { { "lastBlockTime", time } } } }.ToString();
            mh.UpdateData(daoConn.connStr, daoConn.connDB, notifyCounter, updateStr, findStr);
        }
        private long queryLastUpdateTime()
        {
            var key = "orderMarkTime";
            var findStr = new JObject { { "counter", key } }.ToString();
            var queryRes = mh.GetData(daoConn.connStr, daoConn.connDB, notifyCounter, findStr);
            if (queryRes.Count == 0) return -1;

            return long.Parse(queryRes[0]["lastBlockTime"].ToString());
        }

        // #######################
        private void log(string key, long lh, long rh)
        {
            Console.WriteLine("{0}.[{1}]processed: {2}/{3}", name(), key, lh, rh);
        }
        private void updateL(string key, long hh)
        {
            string findStr = new JObject { { "counter", key } }.ToString();
            if (mh.GetDataCount(daoConn.connStr, daoConn.connDB, notifyCounter, findStr) == 0)
            {
                string newdata = new JObject { { "counter", key }, { "lastBlockIndex", hh } }.ToString();
                mh.PutData(daoConn.connStr, daoConn.connDB, notifyCounter, newdata);
                return;
            }
            string updateStr = new JObject { { "$set", new JObject { { "lastBlockIndex", hh } } } }.ToString();
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
            string findStr = new JObject { { "counter", key } }.ToString();
            var queryRes = mh.GetData(daoConn.connStr, daoConn.connDB, notifyCounter, findStr);
            if (queryRes.Count == 0) return 0;
            return long.Parse(queryRes[0]["lastBlockIndex"].ToString());
        }
    }
    public class OrderState
    {
        // 等待付款 + 等待确认 + 等待发货 + 已发货 + 取消订单 + 付款超时 + 交易失败
        public const string WaitingPay = "10141";
        public const string WaitingConfirm = "10142";
        public const string WaitingDeliverGoods = "10143";
        public const string hasDeliverGoods = "10144";
        public const string Canceled = "10145";
        public const string PayTimeout = "10146";
        public const string TxFailed = "10147";
    }
}
