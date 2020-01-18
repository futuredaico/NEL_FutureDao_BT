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
    class ProjMoloContractTaskNewV2Stream : AbsTask
    {
        private MongoDBHelper mh = new MongoDBHelper();
        private DbConnInfo rConn;
        private DbConnInfo lConn;
        private string eventLogsCounter = "counters";
        private string eventLogsCol = "logs";
        private string notifyCounter = "molonotifycounters";
        private string notifyCol = "molonotifyinfos";
        private string projHashCol = "moloprojhashinfos";
        private string projEventCol = "moloprojeventinfos";
        private string projTokenCol = "moloprojtokeninfos";
        private string projInfoCol = "moloprojinfos";
        private string type = Config.getNetType();
        //
        public ProjMoloContractTaskNewV2Stream(string name) : base(name) { }

        public override void initConfig(JObject config)
        {
            rConn = Config.remoteDbConnInfo;
            lConn = Config.localDbConnInfo;

            //addPrefix();
            initIndex();
        }

        private void addPrefix()
        {
            var prefix = "zbak33_";
            notifyCounter = prefix + notifyCounter;
            notifyCol = prefix + notifyCol;
            prefix = "";
            projHashCol = prefix + projHashCol;
            projEventCol = prefix + projEventCol;
            projTokenCol = prefix + projTokenCol;
        }
        private void initIndex()
        {
            mh.setIndex(lConn.connStr, lConn.connDB, notifyCol, "{'blockNumber':1,'event':1}", "i_blockNumber_event");
            mh.setIndex(lConn.connStr, lConn.connDB, notifyCol, "{'transactionHash':1,'event':1}", "i_transactionHash_event");
            mh.setIndex(lConn.connStr, lConn.connDB, notifyCol, "{'counter':1,}", "i_counter");
            mh.setIndex(lConn.connStr, lConn.connDB, projHashCol, "{'contractHash':1,}", "i_contractHash");
            mh.setIndex(lConn.connStr, lConn.connDB, projTokenCol, "{'fundHash':1,}", "i_fundHash");
        }

        public override void process()
        {
            Sleep(2000);
            processNew();
        }
        public void processNew()
        {
            var hashDict = getProjId();
            if (hashDict.Count == 0) return;
            //
            var rh = GetR(out long rt);                         // lastUpdateIndex + timestamp
            var lh = GetL(out long lastCounter, out long lt);   // counter + lastBlockIndex + lastBlockTime + lastCounter
            if (lh >= rh) return;


            var batchSize = 500;
            var findStr = new JObject { { "counter", new JObject { { "$gt", lastCounter } } } }.ToString();
            var sortStr = "{'counter':1}";
            var queryRes = mh.GetData(rConn.connStr, rConn.connDB, eventLogsCol, findStr, sortStr, 0, batchSize);
            if (queryRes.Count == 0)
            {
                UpdateL(-1, rh, rt);
                Log(lastCounter, lh, rh);
                return;
            }
            var res = queryRes.Where(p => hashDict.ContainsKey(p["contractHash"].ToString().ToLower()));
            if (res.Count() == 0)
            {
                UpdateL(-1, rh, rt);
                Log(lastCounter, lh, rh);
                return;
            }
            res = res.Select(p => {
                p["contractHash"] = p["contractHash"].ToString().ToLower();
                return p;
            });
            res = res.Select(p => dataFormat(p)).ToArray();
            res = res.Select(p => dataFormatDecimal(p, hashDict.GetValueOrDefault(p["contractHash"].ToString()))).ToArray();
            res = res.Where(p => p != null).OrderBy(p => long.Parse(p["counter"].ToString())).ToArray();
            foreach (var item in res)
            {
                //findStr = new JObject { { "transactionHash", item["transactionHash"] }, { "event", item["event"] } }.ToString();
                findStr = new JObject { { "counter", item["counter"] }}.ToString();
                if (mh.GetDataCount(lConn.connStr, lConn.connDB, notifyCol, findStr) == 0)
                {
                    mh.PutData(lConn.connStr, lConn.connDB, notifyCol, item.ToString());
                }
            }
            var counter = res.Max(p => long.Parse(p["counter"].ToString()));
            UpdateL(counter, rh, rt);
            Log(counter, lh, rh);
        }

        //
        private class EventInfo
        {
            public string hash { get; set; }
            public string[] fields { get; set; }
            public string[] types { get; set; }
        }
        private Dictionary<string, EventInfo> eventDict;
        private Dictionary<string, EventInfo> getEventInfo()
        {
            if (eventDict == null)
            {
                eventDict = new Dictionary<string, EventInfo>();
                //
                var findStr = "{}";
                var queryRes = mh.GetData(lConn.connStr, lConn.connDB, projEventCol, findStr);
                eventDict = queryRes.ToDictionary(k => k["hash"].ToString(), v => new EventInfo
                {
                    hash = v["hash"].ToString(),
                    fields = ((JArray)v["fields"]).Select(p => p.ToString()).ToArray(),
                    types = ((JArray)v["types"]).Select(p => p.ToString()).ToArray()
                });
            }
            return eventDict;
        }
        private EventInfo getMoloEventInfo(string topics)
        {
            return getEventInfo().GetValueOrDefault(topics);
        }

        //
        private JObject dataFormat(JToken jt, long fundDecimals = 0)
        {
            var eventName = jt["event"].ToString();
            //var types = (jt["argsTypes"] as JArray).ToArray();
            var args = (jt["args"] as JArray).ToArray();
            //var names = MoloEventHelper.getEventParamNames(eventName);
            var topics = ((JArray)jt["topics"]).Select(p => p.ToString()).ToArray()[0];
            var info = getMoloEventInfo(topics);
            if (info == null) return null;

            var names = info.fields;
            var types = info.types;

            var len = args.Count();
            if (len != names.Length || len != types.Length)
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
            data.Add("topics", topics);
            data.Add("counter", jt["counter"]);

            return data;
        }
        private string dataFormat(string type, string val, string name = "", long fundDecimals = 0)
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
        private JObject dataFormatDecimal(JToken data, string projId)
        {
            var newdata = (JObject)data;
            data["projId"] = projId;

            var eventName = data["event"].ToString();
            var topics = data["topics"].ToString();
            if (topics == Topic.Transfer.hash)
            {
                var value = data["value"].ToString();
                var fundHash = data["contractHash"].ToString();
                var fundInfo = getFundInfo(fundHash, projId);
                newdata["valueOrg"] = value;
                newdata["value"] = formatValue(value, fundInfo.decimals);
                newdata["valueDecimals"] = fundInfo.decimals;
                newdata["valueSymbol"] = fundInfo.symbol;
                newdata["valueHash"] = fundHash;
                return newdata;
            }
            if (topics == Topic.Approval.hash)
            {
                var value = data["value"].ToString();
                var fundHash = data["contractHash"].ToString();
                var fundInfo = getFundInfo(fundHash, projId);
                newdata["valueOrg"] = value;
                newdata["value"] = formatValue(value, fundInfo.decimals);
                newdata["valueDecimals"] = fundInfo.decimals;
                newdata["valueSymbol"] = fundInfo.symbol;
                newdata["valueHash"] = fundHash;
                return newdata;
            }

            if (topics == Topic.SubmitProposal.hash)
            {
                var value = data["tokenTribute"].ToString();
                var symbol = getFundSymbolByProjId(projId, out long decimals, out string fundHash);
                newdata["tokenTributeOrg"] = value;
                newdata["tokenTribute"] = formatValue(value, decimals);
                newdata["tokenTributeDecimals"] = decimals;
                newdata["tokenTributeSymbol"] = symbol;
                newdata["tokenTributeHash"] = fundHash;
                return newdata;
            }
            if (topics == Topic.ProcessProposal.hash)
            {
                var value = data["tokenTribute"].ToString();
                var symbol = getFundSymbolByProjId(projId, out long decimals, out string fundHash);
                newdata["tokenTributeOrg"] = value;
                newdata["tokenTribute"] = formatValue(value, decimals);
                newdata["tokenTributeDecimals"] = decimals;
                newdata["tokenTributeSymbol"] = symbol;
                newdata["tokenTributeHash"] = fundHash;
                return newdata;
            }
            if (topics == Topic.Withdrawal.hash)
            {
                var value = data["amount"].ToString();
                var symbol = getFundSymbolByProjId(projId, out long decimals, out string fundHash);
                newdata["amountOrg"] = value;
                newdata["amount"] = formatValue(value, decimals);
                newdata["amountDecimals"] = decimals;
                newdata["amountSymbol"] = symbol;
                newdata["amountHash"] = fundHash;
                return newdata;
            }
            if (topics == Topic.SubmitProposal_v2.hash)
            {
                var value = data["tributeOffered"].ToString();
                var hash = data["tributeToken"].ToString();
                var fundInfo = getFundInfo(hash, projId);
                newdata["tributeOfferedOrg"] = value;
                newdata["tributeOffered"] = formatValue(value, fundInfo.decimals);
                newdata["tributeOfferedDecimals"] = fundInfo.decimals;
                newdata["tributeOfferedSymbol"] = fundInfo.symbol;

                value = data["paymentRequested"].ToString();
                hash = data["paymentToken"].ToString();
                fundInfo = getFundInfo(hash, projId);
                newdata["paymentRequestedOrg"] = value;
                newdata["paymentRequested"] = formatValue(value, fundInfo.decimals);
                newdata["paymentRequestedDecimals"] = fundInfo.decimals;
                newdata["paymentRequestedSymbol"] = fundInfo.symbol;
                return newdata;
            }
            if (topics == Topic.Withdrawal_v2.hash)
            {
                var value = data["amount"].ToString();
                var fundHash = data["tokenAddress"].ToString();
                var fundInfo = getFundInfo(fundHash, projId);
                newdata["amountOrg"] = value;
                newdata["amount"] = formatValue(value, fundInfo.decimals);
                newdata["amountDecimals"] = fundInfo.decimals;
                newdata["amountSymbol"] = fundInfo.symbol;
                return newdata;
            }
            return newdata;
        }
        private string formatValue(string numStr, long decimals)
        {
            var div = new BigInteger(Math.Pow(10, decimals));
            var res = decimal.Parse(numStr) / decimal.Parse(div.ToString());
            return res.ToString();
        }

        //
        private string getFundSymbolByProjId(string projId, out long decimals, out string fundHash)
        {
            decimals = 0L;
            fundHash = "";
            var findStr = new JObject { { "projId", projId } }.ToString();
            var queryRes = mh.GetData(lConn.connStr, lConn.connDB, projInfoCol, findStr);
            if (queryRes.Count == 0) return "";

            var item = queryRes[0];
            decimals = long.Parse(item["fundDecimals"].ToString());
            fundHash = item["fundHash"].ToString();
            var symbol = item["fundSymbol"].ToString();
            return symbol;
        }


        private class FundInfo
        {
            public string hash { get; set; }
            public string symbol { get; set; }
            public long decimals { get; set; }
        }
        private FundInfo getFundInfo(string hash, string projId)
        {
            var info = getFundInfoFromDB(hash);
            if (info != null) return info;

            info = getFundInfoFromChain(hash);
            if (info != null) putFundInfoToDB(info, projId);
            return info;
        }
        private FundInfo getFundInfoFromDB(string hash)
        {
            if (hash == "0x0000000000000000000000000000000000000000")
            {
                return new FundInfo
                {
                    hash = hash,
                    symbol = "eth",
                    decimals = 18
                };
            }
            var findStr = new JObject { { "fundHash", hash } }.ToString();
            var queryRes = mh.GetData(lConn.connStr, lConn.connDB, projTokenCol, findStr);
            if (queryRes.Count == 0) return null;

            var item = queryRes[0];
            var res = new FundInfo
            {
                hash = hash,
                symbol = item["fundSymbol"].ToString(),
                decimals = long.Parse(item["fundDecimals"].ToString())
            };
            return res;
        }
        private FundInfo getFundInfoFromChain(string hash)
        {
            var symbol = EthHelper.getTokenSymbol(hash, type);
            var decimals = EthHelper.getTokenDecimals(hash, type);
            var res = new FundInfo
            {
                hash = hash,
                symbol = symbol,
                decimals = decimals
            };
            return res;
        }
        private void putFundInfoToDB(FundInfo info, string projId)
        {
            var newdata = new JObject {
                { "projId", projId},
                { "fundHash", info.hash},
                { "fundSymbol", info.symbol},
                { "fundDecimals", info.decimals},
                { "fundTotal", "0"},
                { "fundTotalTp", "0"}
            }.ToString();
            mh.PutData(lConn.connStr, lConn.connDB, projTokenCol, newdata);
        }

        //
        private Dictionary<string, string> getProjId()
        {
            var findStr = "{}";
            var queryRes = mh.GetData(lConn.connStr, lConn.connDB, projHashCol, findStr);
            if (queryRes.Count == 0)
            {
                return new Dictionary<string, string>();
            }
            var res = queryRes.ToDictionary(k => k["contractHash"].ToString().ToLower(), v => v["projId"].ToString());
            return res;
        }

        //
        private void Log(long lastCounter, long lh, long rh, string key = "logs")
        {
            Console.WriteLine("{0}.[{1}] processed: {2}-{3}/{4}", name(), key, lastCounter, lh, rh);
        }
        private void UpdateL(long lastCounter, long index, long time, string key = "logs")
        {
            var findStr = new JObject { { "counter", key } }.ToString();
            if (mh.GetDataCount(lConn.connStr, lConn.connDB, notifyCounter, findStr) == 0)
            {
                var newdata = new JObject {
                    { "counter", key },
                    { "lastCounter", lastCounter},
                    { "lastBlockIndex", index},
                    { "lastBlockTime", time}
                }.ToString();
                mh.PutData(lConn.connStr, lConn.connDB, notifyCounter, newdata);
                return;
            }
            var updateJo = new JObject { { "lastBlockIndex", index }, { "lastBlockTime", time } };
            if (lastCounter > -1) updateJo.Add("lastCounter", lastCounter);
            var updateStr = new JObject { { "$set", updateJo } }.ToString();
            mh.UpdateData(lConn.connStr, lConn.connDB, notifyCounter, updateStr, findStr);
        }
        private long GetL(out long lastCounter, out long time, string key = "logs")
        {
            lastCounter = -1L;
            time = 0L;
            var findStr = new JObject { { "counter", key } }.ToString();
            var queryRes = mh.GetData(lConn.connStr, lConn.connDB, notifyCounter, findStr);
            if (queryRes.Count == 0) return -1;

            lastCounter = long.Parse(queryRes[0]["lastCounter"].ToString());
            time = long.Parse(queryRes[0]["lastBlockTime"].ToString());
            return long.Parse(queryRes[0]["lastBlockIndex"].ToString());
        }
        private long GetR(out long time, string key = "transaction")
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
