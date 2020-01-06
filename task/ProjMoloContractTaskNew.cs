using NEL.NNS.lib;
using NEL_FutureDao_BT.core;
using NEL_FutureDao_BT.lib;
using Newtonsoft.Json.Linq;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Numerics;

namespace NEL_FutureDao_BT.task
{
    class ProjMoloContractTaskNew : AbsTask
    {
        private MongoDBHelper mh = new MongoDBHelper();
        private DbConnInfo rConn;
        private DbConnInfo lConn;
        private string eventLogsCounter = "counters";
        private string eventLogsCol = "logs";
        private string notifyCounter = "molonotifycounters";
        private string notifyCol = "molonotifyinfos";
        private string projHashCol = "moloprojhashinfos";
        //
        public ProjMoloContractTaskNew(string name) : base(name) { }

        public override void initConfig(JObject config)
        {
            rConn = Config.remoteDbConnInfo;
            lConn = Config.localDbConnInfo;

            //addPrefix();
            initIndex();

            // 追赶线程
            System.Threading.Tasks.Task.Run(SyncTask);
        }
        private void addPrefix()
        {
            var prefix = "zbak10_";
            notifyCounter = prefix + notifyCounter;
            notifyCol = prefix + notifyCol;
            projHashCol = prefix + projHashCol;
        }
        private void initIndex()
        {
            mh.setIndex(lConn.connStr, lConn.connDB, notifyCol, "{'blockNumber':1,'event':1}", "i_blockNumber_event");
            mh.setIndex(lConn.connStr, lConn.connDB, notifyCol, "{'transactionHash':1,'event':1}", "i_transactionHash_event");
            mh.setIndex(lConn.connStr, lConn.connDB, notifyCol, "{'blockNumber':1,'projId':1}", "i_blockNumber_projId");
            mh.setIndex(lConn.connStr, lConn.connDB, notifyCol, "{'projId':1,'event':1}", "i_projId_event");
        }

        public override void process()
        {
            Sleep(2000);
            var rh = GetR("transaction", out long rt);
            var lh = GetL();
            if (lh >= rh)
            {
                Log(lh, rh);
                return;
            }
            //
            handleHasSyncFinish();
            //
            var hashDict = getProjId(StageType.Normal, out Dictionary<string, long> hashDecimalsDict);
            if (hashDict.Count == 0) return;
            var projIdArr = hashDict.Values.ToArray().Distinct().ToArray();

            if (lh == -1)
            {
                lh = getFirstBlockNumber(hashDict.Keys.ToArray());
            }
            //
            var batchSize = 1000;
            var isNeedUpdateTime = false;
            for (var startIndex = lh; startIndex <= rh; startIndex += batchSize)
            {
                var nextIndex = startIndex + batchSize;
                if (nextIndex > rh) nextIndex = rh;

                var findStr = new JObject { { "blockNumber", new JObject { { "$gt", startIndex }, { "$lte", nextIndex } } } }.ToString();
                var queryRes = mh.GetData(rConn.connStr, rConn.connDB, eventLogsCol, findStr);
                if (queryRes.Count == 0)
                {
                    isNeedUpdateTime = true;
                    continue;
                }
                var res = queryRes.Where(p => hashDict.ContainsKey(p["contractHash"].ToString().ToLower()));
                if (res.Count() == 0)
                {
                    isNeedUpdateTime = true;
                    continue;
                }

                //
                res = res.Select(p => {
                    p["contractHash"] = p["contractHash"].ToString().ToLower();
                    return p;
                });
                res = res.Select(p => dataFormat(p, hashDecimalsDict.GetValueOrDefault(p["contractHash"].ToString()))).ToArray();
                res = res.Where(p => p != null).OrderBy(p => long.Parse(p["blockNumber"].ToString())).ToArray();
                foreach (var item in res)
                {
                    findStr = new JObject { { "transactionHash", item["transactionHash"] }, { "event", item["event"] } }.ToString();
                    if (mh.GetDataCount(lConn.connStr, lConn.connDB, notifyCol, findStr) == 0)
                    {
                        item["projId"] = hashDict.GetValueOrDefault(item["contractHash"].ToString());
                        mh.PutData(lConn.connStr, lConn.connDB, notifyCol, item.ToString());
                    }
                }
                //
                var time = res.Sum(p => long.Parse(p["blockTime"].ToString()));
                UpdateLNew(nextIndex, time, projIdArr);
                Log(nextIndex, rh);
                isNeedUpdateTime = false;
            }
            if (isNeedUpdateTime)
            {
                UpdateLNew(rh, rt, projIdArr);
                Log(rh, rh);
            }
        }
        //
        private void UpdateLNew(long index, long time, string[] projIdArr)
        {
            UpdateL(index, time, "logs", StageType.Normal);
            UpdateLProj(index, time, projIdArr);
        }
        private void UpdateLProj(long index, long time, string[] projIdArr)
        {
            foreach (var projId in projIdArr)
            {
                UpdateL(index, time, projId, StageType.Normal);
            }
        }


        //
        private JObject dataFormat(JToken jt, long fundDecimals)
        {
            var eventName = jt["event"].ToString();
            var types = (jt["argsTypes"] as JArray).ToArray();
            var args = (jt["args"] as JArray).ToArray();
            var names = MoloEventHelper.getEventParamNames(eventName);
            if (names == null || names.Length == 0) return null;

            int len = names.Length;
            if (len != types.Length || len != args.Length)
            {
                throw new Exception("event params not equal");
            }
            var data = new JObject();
            for (int i = 0; i < len; ++i)
            {
                var type = types[i].ToString();
                var arg = args[i].ToString();
                var name = names[i];
                data.Add(name, dataFormat(type, arg, name, fundDecimals));
            }
            data.Add("blockNumber", jt["blockNumber"]);
            data.Add("blockTime", jt["timestamp"]);
            data.Add("transactionHash", jt["transactionHash"]);
            data.Add("contractHash", jt["contractHash"].ToString());
            data.Add("address", jt["address"]);
            data.Add("event", eventName);

            return data;
        }
        private string dataFormat(string type, string val, string name = "", long fundDecimals = 1)
        {
            var div = BigInteger.One;
            if (fundDecimals > 0)
            {
                if (name == "tokenTribute" || name == "amount")
                {
                    div = new BigInteger(Math.Pow(10, fundDecimals));
                }
            }

            type = type.ToLower();
            if (type == "address")
            {
                return "0x" + val.Substring(26);
            }
            if (type == "uint256")
            {
                var num = new BigInteger(val.Substring(2).HexString2Bytes().Reverse().ToArray());
                var res = decimal.Parse(num.ToString()) / decimal.Parse(div.ToString());
                return res.ToString();
                //return (new BigInteger(val.Substring(2).HexString2Bytes().Reverse().ToArray())/div).ToString();
            }
            if (type == "uint8")
            {
                return (new BigInteger(val.Substring(2).HexString2Bytes().Reverse().ToArray()) / div).ToString();
            }
            if (type == "bool")
            {
                return new BigInteger(val.Substring(2).HexString2Bytes().Reverse().ToArray()).ToString();
            }
            throw new Exception("Not support type:" + type);
        }
        private Dictionary<string, string> getProjId(string type, out Dictionary<string, long> hashDecimalsDict)
        {
            var findStr = new JObject { { "type", type } }.ToString();
            var queryRes = mh.GetData(lConn.connStr, lConn.connDB, projHashCol, findStr);
            if (queryRes.Count == 0)
            {
                hashDecimalsDict = new Dictionary<string, long>();
                return new Dictionary<string, string>();
            }
            //
            hashDecimalsDict =
                queryRes.ToDictionary(k => k["contractHash"].ToString().ToLower(), v => long.Parse(v["fundDecimals"].ToString()));
            //
            return queryRes.ToDictionary(k => k["contractHash"].ToString().ToLower(), v => v["projId"].ToString());
        }
        private long getFirstBlockNumber(string[] hashArr)
        {
            var index = 0L;
            try
            {
                index = hashArr.Select(p => getFirstBlockNumber(p)).Where(p => p > 0).Min(p => p);
                --index;
            }
            catch { }
            return index;
        }
        private long getFirstBlockNumber(string hash)
        {
            var findStr = new JObject { { "contractHash", new JObject { { "$regex", "^" + hash + "$" }, { "$options", "i" } } } }.ToString();
            var sortStr = "{'blockNumber':1}";
            var queryRes = mh.GetData(rConn.connStr, lConn.connDB, eventLogsCol, findStr, sortStr, 0, 1);
            if (queryRes.Count == 0) return 0;

            var item = queryRes[0];
            return long.Parse(item["blockNumber"].ToString());
        }

        //
        // SyncTask(Waiting --> finished)
        private void SyncTask()
        {
            while (true)
            {
                Log(2000);
                try
                {
                    SyncTaskLoop();
                }
                catch (Exception ex)
                {
                    Console.WriteLine(ex);
                    Sleep(1000 * 10);
                }
            }
        }
        private void SyncTaskLoop()
        {
            var hashDict = getProjId(StageType.Waiting, out Dictionary<string, long> hashDecimalsDict);
            if (hashDict.Count == 0) return;
            var projIdArr = hashDict.Values.ToArray().Distinct().ToArray();
            //
            var lh = projIdArr.Min(p => GetL(p));
            if (lh == -1)
            {
                lh = getFirstBlockNumber(hashDict.Keys.ToArray()); ;
            }
            var rh = GetL();
            if (rh == -1) rh = GetR("transaction", out long rt);
            if (lh >= rh)
            {
                return;
            }
            //
            var batchSize = 1000;
            processBatch(rh, lh, projIdArr, hashDict, hashDecimalsDict, batchSize, true);
            updateType(projIdArr, StageType.Finished);
            Console.WriteLine("Sync event logs finished:" + projIdArr);
        }


        //
        // HasSyncFinish(finished --> normal)
        private void handleHasSyncFinish()
        {
            var hashDict = getProjId(StageType.Finished, out Dictionary<string, long> hashDecimalsDict);
            if (hashDict.Count == 0) return;
            var projIdArr = hashDict.Values.ToArray().Distinct().ToArray();
            //
            var lh = projIdArr.Min(p => GetL(p));
            var rh = GetL();
            if (lh >= rh)
            {
                updateType(projIdArr, StageType.Normal);
                return;
            }
            var batchSize = 100;
            processBatch(rh, lh, projIdArr, hashDict, hashDecimalsDict, batchSize, false);
            updateType(projIdArr, StageType.Normal);
        }

        //
        private void processBatch(long rh, long lh, string[] projIdArr,
            Dictionary<string, string> hashDict, Dictionary<string, long> hashDecimalsDict, long batchSize, bool isUpdateState)
        {
            var stage = isUpdateState ? StageType.Handling : "";
            var isNeedUpdateTime = false;
            for (var startIndex = lh; startIndex <= rh; startIndex += batchSize)
            {
                var nextIndex = startIndex + batchSize;
                if (nextIndex > rh) nextIndex = rh;

                var findStr = new JObject { { "blockNumber", new JObject { { "$gt", startIndex }, { "$lte", nextIndex } } } }.ToString();
                var queryRes = mh.GetData(rConn.connStr, rConn.connDB, eventLogsCol, findStr);
                if (queryRes.Count == 0)
                {
                    isNeedUpdateTime = false;
                    continue;
                }
                var res = queryRes.Where(p => hashDict.ContainsKey(p["contractHash"].ToString().ToLower()));
                if (res.Count() == 0)
                {
                    isNeedUpdateTime = false;
                    continue;
                }

                //
                res = res.Select(p => {
                    p["contractHash"] = p["contractHash"].ToString().ToLower();
                    return p;
                });
                res = res.Select(p => dataFormat(p, hashDecimalsDict.GetValueOrDefault(p["contractHash"].ToString()))).ToArray();
                res = res.Where(p => p != null).OrderBy(p => long.Parse(p["blockNumber"].ToString())).ToArray();
                foreach (var item in res)
                {
                    findStr = new JObject { { "transactionHash", item["transactionHash"] }, { "event", item["event"] } }.ToString();
                    if (mh.GetDataCount(lConn.connStr, lConn.connDB, notifyCol, findStr) == 0)
                    {
                        item["projId"] = hashDict.GetValueOrDefault(item["contractHash"].ToString());
                        mh.PutData(lConn.connStr, lConn.connDB, notifyCol, item.ToString());
                    }
                }
                //
                var time = res.Sum(p => long.Parse(p["blockTime"].ToString()));
                UpdateLProjAndStage(nextIndex, time, projIdArr, stage);

                Log(nextIndex, rh);
                isNeedUpdateTime = false;
            }
            if (isNeedUpdateTime)
            {
                UpdateLProjAndStage(rh, 0, projIdArr, stage);
                Log(rh, rh);
            }
        }
        private void UpdateLProjAndStage(long index, long time, string[] projIdArr, string stage)
        {
            foreach (var projId in projIdArr)
            {
                UpdateL(index, time, projId, stage);
            }
        }
        private void updateType(string[] projIdArr, string type)
        {
            foreach (var projId in projIdArr)
            {
                updateType(projId, type);
            }
        }
        private void updateType(string projId, string type)
        {
            var findStr = new JObject { { "projId", projId } }.ToString();
            var updateStr = new JObject { { "$set", new JObject { { "type", type } } } }.ToString();
            mh.UpdateDataMany(lConn.connStr, lConn.connDB, projHashCol, updateStr, findStr);
        }
        class StageType
        {
            public const string Normal = "0";
            public const string Waiting = "1";
            public const string Handling = "5";
            public const string Finished = "6";
        }


        //
        private void Log(long lh, long rh, string key = "logs")
        {
            Console.WriteLine("{0}.[{1}] processed: {2}/{3}", name(), key, lh, rh);
        }
        private void UpdateL(long index, long time, string key = "logs", string stage = "")
        {
            var findStr = new JObject { { "counter", key } }.ToString();
            if (mh.GetDataCount(lConn.connStr, lConn.connDB, notifyCounter, findStr) == 0)
            {
                var newdata = new JObject { { "counter", key }, { "lastBlockIndex", index }, { "lastBlockTime", time }, { "stage", stage } }.ToString();
                mh.PutData(lConn.connStr, lConn.connDB, notifyCounter, newdata);
                return;
            }
            var updateJo = new JObject { { "lastBlockIndex", index } };
            if (time > 0) updateJo.Add("lastBlockTime", time);
            if (stage != "") updateJo.Add("stage", stage);
            var updateStr = new JObject { { "$set", updateJo } }.ToString();
            mh.UpdateData(lConn.connStr, lConn.connDB, notifyCounter, updateStr, findStr);

        }
        private long GetL(string key = "logs")
        {

            var findStr = new JObject { { "counter", key } }.ToString();
            var queryRes = mh.GetData(lConn.connStr, lConn.connDB, notifyCounter, findStr);
            if (queryRes.Count == 0) return -1;
            return long.Parse(queryRes[0]["lastBlockIndex"].ToString());
        }
        //private long GetR(string key="transaction")
        private long GetR(string key, out long time)
        {
            time = -1;
            var findStr = new JObject { { "counter", key } }.ToString();
            var queryRes = mh.GetData(rConn.connStr, rConn.connDB, eventLogsCounter, findStr);
            if (queryRes.Count == 0) return -1;
            time = long.Parse(queryRes[0]["timestamp"].ToString());
            return long.Parse(queryRes[0]["lastUpdateIndex"].ToString());
        }
    }

}
