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
    class ProjContractTask : AbsTask
    {
        private MongoDBHelper mh = new MongoDBHelper();
        private DbConnInfo rConn;
        private DbConnInfo lConn;
        private DbConnInfo blockConn;
        private string eventLogsCounter = "counters";
        private string eventLogsCol = "logs";
        private string notifyCounter = "daonotifycounters";
        private string notifyCol = "daonotifyinfos";
        //
        private int sleepInterval = 2000; // 2s

        public ProjContractTask(string name) : base(name) { }

        public override void initConfig(JObject config)
        {
            rConn = Config.remoteDbConnInfo;
            lConn = Config.localDbConnInfo;
            blockConn = rConn;

            initIndex();
        }
        private void initIndex()
        {
            mh.setIndex(lConn.connStr, lConn.connDB, notifyCol, "{'blockNumber':1,'event':1}", "i_blockNumber_event");
            mh.setIndex(lConn.connStr, lConn.connDB, notifyCol, "{'transactionHash':1,'event':1}", "i_transactionHash_event");
        }

        public override void process()
        {
            Sleep(sleepInterval);

            var rh = GetR();
            var lh = GetL();
            if(lh >= rh)
            {
                Log(lh, rh);
                return;
            }
            for(var index=lh+1; index<=rh; ++index)
            {
                var blockTime = GetBlockTime(index);
                //
                var findStr = new JObject { { "blockNumber", index } }.ToString();
                var queryRes = mh.GetData(rConn.connStr, rConn.connDB, eventLogsCol, findStr);
                if(queryRes.Count == 0)
                {
                    UpdateL(index, blockTime);
                    Log(index, rh);
                    continue;
                }

                //var blockTime = GetBlockTime(index);
                var hashArr = queryRes.Select(p => p["contractHash"].ToString()).Distinct().ToArray();
                var hashDict = getProjId(hashArr);
                var res = queryRes.Select(p => dataFormat(p)).ToArray() ;
                foreach(var item in res)
                {
                    if (item == null) continue;
                    findStr = new JObject { { "transactionHash", item["transactionHash"] },{ "event", item["event"]} }.ToString();
                    if (mh.GetDataCount(lConn.connStr, lConn.connDB, notifyCol, findStr) == 0)
                    {
                        item["blockTime"] = blockTime;
                        item["projId"] = hashDict.GetValueOrDefault(item["contractHash"].ToString());
                        mh.PutData(lConn.connStr, lConn.connDB, notifyCol, item);
                    }
                }
                UpdateL(index, blockTime);
                Log(index, rh);
            }
        }

        private JObject dataFormat(JToken jt)
        {
            var eventName = jt["event"].ToString();
            var types = (jt["argsTypes"] as JArray).ToArray();
            var args = (jt["args"] as JArray).ToArray();
            var names = EventHelper.getEventParamNames(eventName);
            if(names == null || names.Length == 0) return null;

            int len = types.Length;
            if(len != args.Length && len != names.Length)
            {
                throw new Exception("event params not equal");
            }
            var data = new JObject ();
            for(int i=0; i<len; ++i)
            {
                var type = types[i].ToString();
                var arg = args[i].ToString();
                var name = names[i];
                data.Add(name, dataFormat(type, arg));
            }
            data.Add("blockNumber", jt["blockNumber"]);
            data.Add("transactionHash", jt["transactionHash"]);
            data.Add("contractHash", jt["contractHash"]);
            data.Add("address", jt["address"]);
            data.Add("event", eventName);
            
            return data;
        }

        private string dataFormat(string type, string val)
        {
            type = type.ToLower();
            if(type == "address")
            {
                return "0x" + val.Substring(26);
            }
            if(type == "uint256")
            {
                return new BigInteger(val.Substring(2).HexString2Bytes().Reverse().ToArray()).ToString();
            }
            throw new Exception("Not support type:"+type);
        }


        private Dictionary<string, string> getProjId(string[] hashArr)
        {
            var findStr = MongoFieldHelper.toFilter(hashArr, "contractHash").ToString();
            var queryRes = mh.GetData(lConn.connStr, lConn.connDB, "daoprojfinancehashinfos", findStr);
            return hashArr.ToDictionary(p => p, p => {
                var r = queryRes.Where(pq => pq["contractHash"].ToString() == p).First();
                if (r == null) throw new Exception("Not find pid by hash:" + p);
                return r["projId"].ToString();
            });
        }
        private long GetBlockTime(long blockNumber)
        {
            var findStr = new JObject { { "number", blockNumber } }.ToString();
            var fieldStr = new JObject { { "timestamp", 1 } }.ToString();
            var queryRes = mh.GetData(blockConn.connStr, blockConn.connDB, "blocks", findStr, fieldStr);
            if (queryRes.Count == 0) return -1;
            return long.Parse(queryRes[0]["timestamp"].ToString());
        }

        private void Log(long lh, long rh, string key = "logs")
        {
            Console.WriteLine("{0}.[{1}]processed: {2}/{3}", name(), key, lh, rh);
        }
        private void UpdateL(long index, long time, string key="logs")
        {
            var findStr = new JObject { { "counter", key } }.ToString();
            if(mh.GetDataCount(lConn.connStr, lConn.connDB, notifyCounter, findStr) == 0)
            {
                var newdata = new JObject { { "counter", key },{ "lastBlockIndex", index},{ "lastBlockTime", time} }.ToString();
                mh.PutData(lConn.connStr, lConn.connDB, notifyCounter, newdata);
                return;
            }
            var updateStr = new JObject { { "$set", new JObject { { "lastBlockIndex", index },{ "lastBlockTime", time} } } }.ToString();
            mh.UpdateData(lConn.connStr, lConn.connDB, notifyCounter, updateStr, findStr);

        }
        private long GetL(string key="logs")
        {
            var findStr = new JObject { { "counter", key } }.ToString();
            var queryRes = mh.GetData(lConn.connStr, lConn.connDB, notifyCounter, findStr);
            if (queryRes.Count == 0) return -1;
            return long.Parse(queryRes[0]["lastBlockIndex"].ToString());
        }
        private long GetR(string key="transaction")
        {
            var findStr = new JObject { { "counter", key } }.ToString();
            var queryRes = mh.GetData(rConn.connStr, rConn.connDB, eventLogsCounter, findStr);
            if (queryRes.Count == 0) return -1;
            return long.Parse(queryRes[0]["lastUpdateIndex"].ToString());
        }

    }

    class EventHelper
    {
        private static Dictionary<string, string[]> eventParamNames = new Dictionary<string, string[]>
        {
            // 管理合约 ************
            { "OnChangeSlope", new string[]{ "slope"} },      // 斜率   默认: 1000 * 10^9，  实际需除以1000
            { "OnChangeAlpha", new string[]{ "alpha"} },    // ratio  默认: 300， 实际需除以1000


            // 资金池合约 *********
            // 预挖矿
            { "OnPreMint", new string[]{"who", "tokenAmt", "timestamp" } },
            { "OnBuy", new string[]{ "who", "fundAmt", "tokenAmt"} },
            { "OnSell", new string[]{ "who", "fundAmt", "tokenAmt"} },
            // 项目方取钱
            { "OnSendEth", new string[]{ "who", "fundAmt"} },
            // 投资方清退
            { "OnClearing", new string[]{ "who", "ratio", "fundAmt30", "fundAmt70"} },
            // 项目方盈利
            { "OnRevenue", new string[]{ "who", "fundAmt"} },
            // 订单支付确认
            { "OnEvent", new string[]{ "orderId"} },
            

            // 自治合约 *********
            { "OnGetFdtOut", new string[]{ "who", "amt"} },
            { "OnSetFdtIn", new string[]{ "who", "amt"} },
            { "OnLock", new string[]{ "contractAddress", "lockAddress", "index", "expireDate", "lockAmount"} },
            { "OnFree", new string[]{ "contractAddress", "lockAddress", "index", "expireDate"} },

            // 代币合约 *********
            { "Transfer", new string[]{ "from", "to", "amt"} },
            { "Approval", new string[]{ "owner", "spender", "amt"} },
            //
            { "OnApplyProposal", new string[]{ "index", "proposalName", "proposaler", "startTime", "recipient", "fundAmt","timeConsuming","proposalDetail"} },
            { "OnVote", new string[]{ "who", "index", "voteResult", "shares"} },
            { "OnProcess", new string[]{ "index", "pass"} },
            { "OnAbort", new string[]{ "index"} },
            { "OnOneTicketRefuse", new string[]{ "index"} },
            //
            { "OnApplyClearingProposal", new string[]{ "index", "proposaler", "address", "startTime"} },
            { "OnVoteClearingProposal", new string[]{ "who", "index", "fdtAmt"} },
            { "OnProcessClearingProposal", new string[]{ "index", "pass"} }
        };

        public static string[] getEventParamNames(string eventName)
        {
            return eventParamNames.GetValueOrDefault(eventName);
        }
    }
}
