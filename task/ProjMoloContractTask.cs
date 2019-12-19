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
    class ProjMoloContractTask : AbsTask
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
        public ProjMoloContractTask(string name) : base(name) { }

        public override void initConfig(JObject config)
        {
            rConn = Config.remoteDbConnInfo;
            lConn = Config.localDbConnInfo;

            initIndex();
        }
        private void initIndex()
        {
            mh.setIndex(lConn.connStr, lConn.connDB, notifyCol, "{'blockNumber':1,'event':1}", "i_blockNumber_event");
            mh.setIndex(lConn.connStr, lConn.connDB, notifyCol, "{'transactionHash':1,'event':1}", "i_transactionHash_event");
        }

        private string[] nilArr = new string[0];
        public override void process()
        {
            Sleep(2000);

            var rh = GetR();
            var lh = GetL();
            if (lh >= rh)
            {
                Log(lh, rh);
                return;
            }
            //foreach(var hh in cc.OrderBy(p => p).ToArray())
            //{
            //    var index = hh;
            for (var index = lh + 1; index <= rh; ++index)
            {
                var findStr = new JObject { { "blockNumber", index } }.ToString();
                var queryRes = mh.GetData(rConn.connStr, rConn.connDB, eventLogsCol, findStr);
                if (queryRes.Count == 0)
                {
                    UpdateL(index, 0);
                    UpdateL(index, 0, nilArr);
                    Log(index, rh);
                    continue;
                }
                var blockTime = long.Parse(queryRes[0]["timestamp"].ToString());

                var hashArr = queryRes.Select(p => p["contractHash"].ToString()).Distinct().ToArray();
                var hashDict = getProjId(hashArr, out Dictionary<string, string> hashTypeDict, out Dictionary<string, long> hashDecimalsDict);
                if (hashDict.Count == 0) continue;
                if (hashTypeDict.Count > 0)
                {
                    processNewContract(index, hashTypeDict, hashDecimalsDict);
                }
                var res = queryRes.Where(p => hashDict.ContainsKey(p["contractHash"].ToString().ToLower())).ToArray();
                if (res.Count() == 0) continue;
                res = queryRes.Select(p => dataFormat(p, hashDecimalsDict.GetValueOrDefault(p["contractHash"].ToString()))).ToArray();
                foreach (var item in res)
                {
                    if (item == null) continue;
                    item["contractHash"] = item["contractHash"].ToString().ToLower();
                    findStr = new JObject { { "transactionHash", item["transactionHash"] }, { "event", item["event"] } }.ToString();
                    if (mh.GetDataCount(lConn.connStr, lConn.connDB, notifyCol, findStr) == 0)
                    {
                        item["blockTime"] = blockTime;
                        item["projId"] = hashDict.GetValueOrDefault(item["contractHash"].ToString());
                        mh.PutData(lConn.connStr, lConn.connDB, notifyCol, item.ToString());
                    }
                }
                UpdateL(index, blockTime);
                UpdateL(index, blockTime, hashDict.Values.Distinct().ToArray());
                Log(index, rh);
            }
        }

        private void UpdateL(long index, long time, string[] hashArr)
        {
            if (time == 0) return;
            var findStr = "{}";
            var queryRes = mh.GetData(lConn.connStr, lConn.connDB, notifyCounter, findStr);
            //
            string[] hasHashArr = null;
            string[] notHashArr = null;
            if (queryRes.Count == 0)
            {
                notHashArr = hashArr;
            } else
            {
                var counterArr = queryRes.Select(p => p["counter"].ToString());
                hasHashArr = counterArr.ToArray();
                if(hashArr != null && hashArr.Count() > 0)
                {
                    hasHashArr = hashArr.Where(p => counterArr.Contains(p)).ToArray();
                    notHashArr = hashArr.Where(p => !counterArr.Contains(p)).ToArray();
                }
            }
            if(hasHashArr != null && hasHashArr.Count() > 0)
            {
                var updateStr = new JObject { { "$set", new JObject { { "lastBlockIndex", index }, { "lastBlockTime", time } } } }.ToString();
                foreach (var item in hasHashArr)
                {
                    findStr = new JObject { { "counter", item } }.ToString();
                    mh.UpdateData(lConn.connStr, lConn.connDB, notifyCounter, updateStr, findStr);
                }
            }
            if (notHashArr != null && notHashArr.Count() > 0)
            {
                var joArr = notHashArr.Select(p => new JObject { { "counter", p }, { "lastBlockIndex", index }, { "lastBlockTime", time } }).ToArray();
                mh.PutData(lConn.connStr, lConn.connDB, notifyCounter, new JArray { joArr });
            }
        }
        long[] cc = new long[]
        {
            9055683,
    8983158,
    8983036,
    8979413,
    8976848,
    8974625,
    8972591,
    8884789,
    8884765,
    8884755,
    8884724,
    8756471,
    8756465,
    8719087,
    8688915,
    8688912,
    8655222,
    8652319,
    8652162,
    8649694,
    8649160,
    8647384,
    8641989,
    8641705,
    8637521,
    8621015,
    8612217,
    8602592,
    8602587,
    8594628,
    8594620,
    8594603,
    8594588,
    8594579,
    8581851,
    8574375,
    8568910,
    8561530,
    8557400,
    8556414,
    8554059,
    8552217,
    8552113,
    8552066,
    8551905,
    8551679,
    8551658,
    8544779,
    8543610,
    8542447,
    8520087,
    8518432,
    8506486,
    8503339,
    8500367,
    8497346,
    8491205,
    8491075,
    8490913,
    8490903,
    8490760,
    8490673,
    8486875,
    8485672,
    8484388,
    8479516,
    8479464,
    8455725,
    8455717,
    8448335,
    8447732,
    8442425,
    8441653,
    8441465,
    8435085,
    8432983,
    8432194,
    8432132,
    8428108,
    8422725,
    8422718,
    8422709,
    8412818,
    8407422,
    8406354,
    8405288,
    8404659,
    8404390,
    8403784,
    8385133,
    8384784,
    8381852,
    8372225,
    8372212,
    8372208,
    8372198,
    8372185,
    8371260,
    8367050,
    8367028,
    8364602,
    8357528,
    8353822,
    8350001,
    8345895,
    8345770,
    8345098,
    8345058,
    8344417,
    8343913,
    8337896,
    8337891,
    8333120,
    8333113,
    8330112,
    8330106,
    8328144,
    8326850,
    8320757,
    8317931,
    8317788,
    8317167,
    8317149,
    8317144,
    8317139,
    8315417,
    8314567,
    8314558,
    8313440,
    8313085,
    8312600,
    8312542,
    8299895,
    8299767,
    8299708,
    8297740,
    8297709,
    8292758,
    8292533,
    8280141,
    8263304,
    8257196,
    8257171,
    8253363,
    8247851,
    8246700,
    8246685,
    8238551,
    8224383,
    8223290,
    8223281,
    8221149,
    8198769,
    8177869,
    8177810
        };
        // **********************************
        private void processNewContract(long index, Dictionary<string, string> newHashDict, Dictionary<string, long> hashDecimalsDict)
        {
            var firstIndex = newHashDict.Keys.Select(p => getFirstBlockIndex(p)).Min();
            for (var startIndex = firstIndex; startIndex < index;) {
                var endIndex = startIndex + 100;
                if (endIndex > index) endIndex = index;
                var findStr = new JObject { { "blockNumber", new JObject { { "$gte", startIndex},{ "$lt", endIndex} } } }.ToString();
                var queryRes = mh.GetData(rConn.connStr, rConn.connDB, eventLogsCol, findStr);
                startIndex += 100;
                if (queryRes.Count == 0) continue;
                //
                var numberArr = queryRes.Select(p => (long)p["blockNumber"]).Distinct().ToArray();
                var res = queryRes.Select(p =>
                {
                    var hash = p["contractHash"].ToString();
                    var dt = dataFormat(p, hashDecimalsDict.GetValueOrDefault(hash));
                    dt["projId"] = newHashDict.GetValueOrDefault(hash);
                    dt["blockTime"] = long.Parse(p["blockTime"].ToString());
                    return dt;
                }).ToArray();
                saveContractInfo(res);
            }
            transferType(newHashDict.Values.Distinct().ToArray());
        }
        private long getFirstBlockIndex(string hash)
        {
            var findStr = new JObject { { "contractHash", hash } }.ToString();
            var sortStr = "{'blockNumber':1}";
            var queryRes = mh.GetData(rConn.connStr, lConn.connDB, eventLogsCol, findStr, sortStr, 0, 1);
            if (queryRes.Count == 0) return 0;

            var item = queryRes[0];
            return long.Parse(item["blockNumber"].ToString());
        }
        private void saveContractInfo(JObject[] jtArr)
        {
            foreach(var item in jtArr)
            {
                if (item == null) continue;
                var findStr = new JObject { { "transactionHash", item["transactionHash"] }, { "event", item["event"] } }.ToString();
                if (mh.GetDataCount(lConn.connStr, lConn.connDB, notifyCol, findStr) == 0)
                {
                    mh.PutData(lConn.connStr, lConn.connDB, notifyCol, item);
                }
            }
        }
        private void transferType(string[] projIdArr)
        {
            foreach(var projId in projIdArr)
            {
                var findStr = new JObject { { "projId", projId } }.ToString();
                var updateStr = new JObject { { "$set", new JObject { { "type", MoloType.Init } } } }.ToString();
                mh.UpdateData(lConn.connStr, lConn.connDB, projHashCol, updateStr, findStr);
            }
        }

        private JObject dataFormat(JToken jt, long fundDecimals)
        {
            var eventName = jt["event"].ToString();
            var types = (jt["argsTypes"] as JArray).ToArray();
            var args = (jt["args"] as JArray).ToArray();
            var names = MoloEventHelper.getEventParamNames(eventName);
            if(names == null || names.Length == 0) return null;

            int len = types.Length;
            if(len != args.Length || len != names.Length)
            {
                throw new Exception("event params not equal");
            }
            var data = new JObject ();
            for(int i=0; i<len; ++i)
            {
                var type = types[i].ToString();
                var arg = args[i].ToString();
                var name = names[i];
                data.Add(name, dataFormat(type, arg, name, fundDecimals));
            }
            data.Add("blockNumber", jt["blockNumber"]);
            data.Add("transactionHash", jt["transactionHash"]);
            data.Add("contractHash", jt["contractHash"]);
            data.Add("address", jt["address"]);
            data.Add("event", eventName);
            
            return data;
        }
        private string dataFormat(string type, string val, string name="", long fundDecimals=1)
        {
            var div = BigInteger.One;
            if (fundDecimals > 0)
            {
                if(name == "tokenTribute" || name == "amount")
                {
                    div = new BigInteger(Math.Pow(10, fundDecimals));
                }
            }

            type = type.ToLower();
            if(type == "address")
            {
                return "0x" + val.Substring(26);
            }
            if(type == "uint256")
            {
                return (new BigInteger(val.Substring(2).HexString2Bytes().Reverse().ToArray())/div).ToString();
            }
            if(type ==  "uint8")
            {
                return (new BigInteger(val.Substring(2).HexString2Bytes().Reverse().ToArray())/div).ToString();
            }
            if(type == "bool")
            {
                return new BigInteger(val.Substring(2).HexString2Bytes().Reverse().ToArray()).ToString();
            }
            throw new Exception("Not support type:"+type);
        }

        private Dictionary<string, string> getProjId(string[] hashArr, out Dictionary<string, string> newProjIdDict, out Dictionary<string, long> hashDecimalsDict)
        {
            var findStr = "{}"; 
            var queryRes = mh.GetData(lConn.connStr, lConn.connDB, projHashCol, findStr);
            if (queryRes.Count == 0)
            {
                newProjIdDict = new Dictionary<string, string>();
                hashDecimalsDict = new Dictionary<string, long>();
                return new Dictionary<string, string>();
            }
            //
            newProjIdDict = 
                queryRes.Where(p => p["type"].ToString() != MoloType.Init)
                .ToDictionary(k => k["contractHash"].ToString().ToLower(), v => v["projId"].ToString());
            //
            hashDecimalsDict =
                queryRes.ToDictionary(k => k["contractHash"].ToString().ToLower(), v => long.Parse(v["fundDecimls"].ToString()));
            //
            return queryRes.ToDictionary(k => k["contractHash"].ToString().ToLower(), v => v["projId"].ToString());
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

    class MoloEventHelper
    {
        private static Dictionary<string, string[]> eventParamNames = new Dictionary<string, string[]>
        {
            /*
    event SubmitProposal(uint256 proposalIndex, address indexed delegateKey, address indexed memberAddress, address indexed applicant, uint256 tokenTribute, uint256 sharesRequested);
    event SubmitVote(uint256 indexed proposalIndex, address indexed delegateKey, address indexed memberAddress, uint8 uintVote);
    event ProcessProposal(uint256 indexed proposalIndex, address indexed applicant, address indexed memberAddress, uint256 tokenTribute, uint256 sharesRequested, bool didPass);
    event Ragequit(address indexed memberAddress, uint256 sharesToBurn);
    event Abort(uint256 indexed proposalIndex, address applicantAddress);
    event UpdateDelegateKey(address indexed memberAddress, address newDelegateKey);
    event SummonComplete(address indexed summoner, uint256 shares);
             */
            // molo.st ********************
            { "SubmitVote", new string[]{ "proposalIndex", "delegateKey", "memberAddress", "uintVote" } },
            { "SubmitProposal", new string[]{ "proposalIndex", "delegateKey", "memberAddress", "applicant", "tokenTribute", "sharesRequested"} },
            { "ProcessProposal", new string[]{ "proposalIndex", "applicant", "memberAddress", "tokenTribute", "sharesRequested", "didPass" } },
            { "Ragequit", new string[]{ "memberAddress", "sharesToBurn" } },
            { "Abort", new string[]{ "proposalIndex", "applicantAddress" } }, // applicantAddress = delegateKey
            { "UpdateDelegateKey", new string[]{ "memberAddress", "newDelegateKey" } },
            { "SummonComplete", new string[]{ "summoner", "shares" } },
            // fund
            //emit Withdrawal(receiver, amount);
            { "Withdrawal", new string[]{ "receiver", "amount" } },

            // molo.ed ********************


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

    class MoloType
    {
        public const string Init = "0";
        public const string Add = "1";
    }
}
