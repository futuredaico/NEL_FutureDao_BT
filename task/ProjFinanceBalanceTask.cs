using MongoDB.Bson;
using NEL.NNS.lib;
using NEL_FutureDao_BT.core;
using NEL_FutureDao_BT.lib;
using NEL_FutureDao_BT.task.dao;
using Newtonsoft.Json.Linq;
using System;
using System.Collections.Generic;
using System.Linq;

namespace NEL_FutureDao_BT.task
{
    class ProjFinanceBalanceTask : AbsTask
    {
        private MongoDBHelper mh = new MongoDBHelper();
        private DbConnInfo daoConn;
        private string notifyCounter = "daonotifycounters";
        private string notifyCol = "daonotifyinfos";
        private string projFinanceBalanceCol = "daoprojfinancebalanceinfos";

        private string projFinanceFundPoolCol = "daoprojfinancefundpoolinfos";
        private string projFinanceCol = "daoprojfinanceinfos";
        private string projProposalVoteCol = "daoprojproposalvoteinfos";

        public ProjFinanceBalanceTask(string name) : base(name) { }

        public override void initConfig(JObject config)
        {
            /// 个人可用代币 = 预留已解锁的 + onBuy - onSell - onVote(投票) + onVote(正常结束/取消/终止)
            /// 个人锁定代币 = 预留未解锁的 + onVote(投票)
            /// 个人代币余额 = 可用 + 锁定

            daoConn = Config.daoDbConnInfo;
        }

        public override void process()
        {
            Sleep(2000);
            handleOperateInfo();
            //
            handleOnPreMintAtEnd();
            //handleOnVoteAtEnd();
        }
        private void handleOperateInfo()
        {
            string key = "balance";
            long rh = getR();
            long lh = getL(key);
            if (lh >= rh)
            {
                log(key, lh, rh);
                return;
            }

            HashSet<string> projIdSet = new HashSet<string>();
            for(var index=lh+1; index<=rh; ++index)
            {
                string findStr = new JObject { { "blockNumber", index } }.ToString();
                var queryRes = mh.GetData(daoConn.connStr, daoConn.connDB, notifyCol, findStr);
                if (queryRes.Count == 0)
                {
                    updateL(key, index);
                    log(key, index, rh);
                    continue;
                }
                //
                var ids = queryRes.Select(p => p["projId"].ToString()).Distinct().ToArray();
                var idDict = getProjTokenName(ids);
                //
                var now = TimeHelper.GetTimeStamp();
                var infoDict = new Dictionary<string, TokenBalanceInfo>();
                foreach(var item in queryRes)
                {
                    var projId = item["projId"].ToString();
                    var eventName = item["event"].ToString();
                    if(eventName == "OnPreMint"
                        || eventName == "OnBuy"
                        || eventName == "OnSell")
                    {
                        var who = item["who"].ToString();
                        var amt = decimal.Parse(item["tokenAmt"].ToString());
                        handleAmt(infoDict, eventName, projId, who, amt);
                    }
                    if(eventName == "Transfer")
                    {
                        var from = item["from"].ToString();
                        var to = item["to"].ToString();
                        var amt = decimal.Parse(item["amt"].ToString());
                        if (amt == 0) continue;
                        handleAmt(infoDict, eventName+"_from", projId, from, amt);
                        handleAmt(infoDict, eventName+"_to", projId, to, amt);
                    }
                    //
                    if (eventName == "OnSetFdtIn" || eventName == "OnGetFdtOut")
                    {
                        var who = item["who"].ToString();
                        var amt = decimal.Parse(item["amt"].ToString());
                        handleAmt(infoDict, eventName, projId, who, amt);
                    }
                    if(eventName == "OnVote")
                    {
                        var who = item["who"].ToString();
                        var amt = decimal.Parse(item["shares"].ToString());
                        handleAmt(infoDict, eventName, projId, who, amt);
                    }
                    if(eventName == "OnProcess" || eventName == "OnAbort" || eventName == "OnOneTicketRefuse")
                    {
                        var proposalIndex = long.Parse(item["index"].ToString());
                        handleOnProcess(infoDict, eventName, projId, proposalIndex);
                    }
                }
                infoDict.Values.Select(p =>
                {
                    p.tokenName = idDict?.GetValueOrDefault(p.projId)?["tokenName"].ToString();
                    p.fundName = idDict?.GetValueOrDefault(p.projId)?["fundName"].ToString();
                    p.lastUpdateTime = now;
                    return p;
                }).ToList().ForEach(p => handleInfo(p));
                //
                updateL(key, index);
                log(key, index, rh);

                // 清除临时字段数据
                fundPoolTempNotClearAllFlag = false;
                var dbZERO = decimal.Zero.format();
                try
                {
                    foreach (var item in infoDict.Values)
                    {
                        string subfindStr = new JObject { { "projId", item.projId },{ "address", item.address} }.ToString();
                        var updateBson = new BsonDocument { { "$set", new BsonDocument{
                            {"onPreMintTp", dbZERO},
                            {"onBuyTp", dbZERO},
                            {"onSellTp", dbZERO},
                            {"transferFromTp", dbZERO},
                            {"transferToTp", dbZERO},
                            {"onSetFdtInTp", dbZERO},
                            {"onGetFdtOutTp", dbZERO},
                            {"onVoteTp", dbZERO},
                        }} };
                        mh.UpdateDecimal(daoConn.connStr, daoConn.connDB, projFinanceBalanceCol, updateBson, subfindStr);
                    }
                }
                catch (Exception ex)
                {
                    // 
                    Console.WriteLine(ex);
                    fundPoolTempNotClearAllFlag = true;
                }
            }

            // 统计支持人数
            handleHasSupport(projIdSet);
        }

        private bool fundPoolTempNotClearAllFlag = false;
        private void handleHasSupport(HashSet<string> ids)
        {
            if (ids.Count == 0) return;

            ids.ToList().ForEach(projId =>
            {
                var findStr = new JObject { { "projId", projId }, { "balance", new JObject { { "$gt", 0 } } } }.ToString();
                var count = mh.GetDataCount(daoConn.connStr, daoConn.connDB, projFinanceBalanceCol, findStr);
                findStr = new JObject { { "projId", projId } }.ToString();
                var fieldStr = new JObject { { "hasSupport", 1 } }.ToString();
                var queryRes = mh.GetData(daoConn.connStr, daoConn.connDB, projFinanceFundPoolCol, findStr, fieldStr);
                if (queryRes.Count == 0) return;
                if (long.Parse(queryRes[0]["hasSupport"].ToString()) == count) return;

                var updateStr = new JObject { { "$set", new JObject { { "hasSupport", count } } } }.ToString();
                mh.UpdateData(daoConn.connStr, daoConn.connDB, projFinanceFundPoolCol, updateStr, findStr);
            });
        }

        //
        private const long OneHourSeconds = 3600;
        private void handleOnPreMintAtEnd()
        {
            var now = TimeHelper.GetTimeStamp();
            var findStr = new JObject { { "onPreMintAtEndFinishFlag", false},{ "onPreMintAtEndTime", new JObject { { "$lt",  now - OneHourSeconds } } } }.ToString();
            var queryRes = mh.GetData<TokenBalanceInfo>(daoConn.connStr, daoConn.connDB, projFinanceBalanceCol, findStr);
            if (queryRes.Count == 0) return;
            foreach(var item in queryRes)
            {
                handleOnPreMintAtEndItem(item, now);
            }
        }
        private void handleOnPreMintAtEndItem(TokenBalanceInfo info, long now)
        {
            var findStr = "";
            var updateStr = "";
            if(info.onPreMint.format() == info.onPreMintAtEnd.format())
            {
                findStr = new JObject { { "projId", info.projId},{ "address", info.address} }.ToString();
                updateStr = new JObject { { "$set", new JObject { { "onPreMintAtEndFinishFlag", true }, { "onPreMintAtEndTime", now } } } }.ToString();
                mh.UpdateData(daoConn.connStr, daoConn.connDB, projFinanceBalanceCol, updateStr, findStr);
                return;
            }
            findStr = new JObject { { "who", info.address},{ "timestamp", new JObject { { "$lt", now} } } }.ToString();
            var fieldStr = new JObject { { "tokenAmt", 1 } }.ToString();
            var queryRes = mh.GetData(daoConn.connStr, daoConn.connDB, projFinanceBalanceCol, findStr, fieldStr);
            if (queryRes.Count == 0) return;

            var unlockAmt = queryRes.Sum(p => decimal.Parse(p["tokenAmt"].ToString()));
            if (info.onPreMintAtEnd.format() == unlockAmt) return;

            var updateJo = new BsonDocument();
            updateJo.Add("onPreMintAtEnd", unlockAmt.format());
            updateJo.Add("onPreMintAtEndTime", now);
            updateJo.Add("lastUpdateTime", now);
            if (info.onPreMint.format() == unlockAmt)
            {
                updateJo.Add("onPreMintAtEndFinishFlag", true);
            }
            var updateBson = new BsonDocument { { "$set", updateJo } };
            mh.UpdateDecimal(daoConn.connStr, daoConn.connDB, projFinanceBalanceCol, updateBson, findStr);
        }


        private void handleOnProcess(Dictionary<string, TokenBalanceInfo> infoDict, string eventName, string projId, long proposalIndex)
        {
            var findStr = new JObject { { "projId", projId }, { "index", proposalIndex } }.ToString();
            var queryRes = mh.GetData(daoConn.connStr, daoConn.connDB, projProposalVoteCol, findStr);
            if (queryRes.Count == 0) return;

            foreach(var item in queryRes)
            {
                var who = item["who"].ToString();
                var amt = decimal.Parse(item["shares"].ToString());
                handleAmt(infoDict, eventName, projId, who, amt);
            }
        }

        // 
        private void handleAmt(Dictionary<string, TokenBalanceInfo> infoDict, string eventName, string projId, string who, decimal amt)
        {
            if (amt == 0) return;
            var k = projId + who;
            var info = infoDict.ContainsKey(k) ? infoDict.GetValueOrDefault(k) : new TokenBalanceInfo { projId = projId, address = who };
            if (eventName == "OnPreMint")
            {
                info.onPreMint = (info.onPreMint.format() + amt).format();
                info.onPreMintTp = info.onPreMint;
                info.onPreMintAtEndFinishFlag = false;
                info.onPreMintAtEndTime = TimeHelper.GetTimeStamp();
                infoDict[k] = info;
                return;
            }
            if (eventName == "OnBuy")
            {
                info.onBuy = (info.onBuy.format() + amt).format();
                info.onBuyTp = info.onBuy;
                infoDict[k] = info;
                return;
            }
            if (eventName == "OnSell")
            {
                info.onSell = (info.onSell.format() + amt).format();
                info.onSellTp = info.onSell;
                infoDict[k] = info;
                return;
            }
            if (eventName == "Transfer_from")
            {
                info.transferFrom = (info.transferFrom.format() + amt).format();
                info.transferFromTp = info.transferFrom;
                infoDict[k] = info;
                return;
            }
            if (eventName == "Transfer_to")
            {
                info.transferTo = (info.transferTo.format() + amt).format();
                info.transferToTp = info.transferTo;
                infoDict[k] = info;
                return;
            }
            if (eventName == "OnSetFdtIn")
            {
                info.onSetFdtIn = (info.onSetFdtIn.format() + amt).format();
                info.onSetFdtInTp = info.onSetFdtIn;
                infoDict[k] = info;
                return;
            }
            if (eventName == "OnGetFdtOut")
            {
                info.onGetFdtOut = (info.onGetFdtOut.format() + amt).format();
                info.onGetFdtOutTp = info.onGetFdtOut;
                infoDict[k] = info;
                return;
            }
            if (eventName == "OnVote")
            {
                info.onVote = (info.onVote.format() + amt).format();
                info.onVoteTp = info.onVote;
                infoDict[k] = info;
                return;
            }
            if(eventName == "OnProcess")
            {
                info.onProcess = (info.onProcess.format() + amt).format();
                info.onProcessTp = info.onProcess;
                infoDict[k] = info;
                return;
            }
        }
        private void handleInfo(TokenBalanceInfo info)
        {
            var findStr = new JObject { { "projId", info.projId }, { "address", info.address } }.ToString();
            var queryRes = mh.GetData<TokenBalanceInfo>(daoConn.connStr, daoConn.connDB, projFinanceBalanceCol, findStr);
            if(queryRes.Count == 0)
            {

                info.balance = (info.onPreMint.format() + info.onBuy.format() - info.onSell.format()).format();
                mh.PutData<TokenBalanceInfo>(daoConn.connStr, daoConn.connDB, projFinanceBalanceCol, info);
                return;
            }
            var item = queryRes[0];
            handleUpdate(item, info, findStr);
        }
        private void handleUpdate(TokenBalanceInfo oldB, TokenBalanceInfo newB, string findStr)
        {
            var zero = decimal.Zero;
            var updateJo = new BsonDocument();
            bool updateFlag = false;
            var onPreMint = oldB.onPreMint.format();
            if(newB.onPreMint != zero)
            {
                var amt = newB.onPreMint.format();
                var amtOld = oldB.onPreMint.format();
                var amtOldTp = oldB.onPreMintTp.format();
                if (fundPoolTempNotClearAllFlag)
                {
                    amtOldTp = decimal.Zero;
                }
                var amtUp = amtOld - amtOldTp + amt;
                var amtUpTp = amt;
                updateJo.Add("onPreMint", amtUp.format());
                updateJo.Add("onPreMintTp", amtUpTp.format());
                updateFlag = true;
                onPreMint = amtUp;
            }
            var onBuy = oldB.onBuy.format();
            if (newB.onBuy != zero)
            {
                var amt = newB.onBuy.format();
                var amtOld = oldB.onBuy.format();
                var amtOldTp = oldB.onBuyTp.format();
                if (fundPoolTempNotClearAllFlag)
                {
                    amtOldTp = decimal.Zero;
                }
                var amtUp = amtOld - amtOldTp + amt;
                var amtUpTp = amt;
                updateJo.Add("onBuy", amtUp.format());
                updateJo.Add("onBuyTp", amtUpTp.format());
                updateFlag = true;
                onBuy = amtUp;
            }
            var onSell = oldB.onSell.format();
            if (newB.onSell != zero)
            {
                var amt = newB.onSell.format();
                var amtOld = oldB.onSell.format();
                var amtOldTp = oldB.onSellTp.format();
                if (fundPoolTempNotClearAllFlag)
                {
                    amtOldTp = decimal.Zero;
                }
                var amtUp = amtOld - amtOldTp + amt;
                var amtUpTp = amt;
                updateJo.Add("onSell", amtUp.format());
                updateJo.Add("onSellTp", amtUpTp.format());
                updateFlag = true;
                onSell = amtUp;
            }
            if(updateFlag)
            {
                var balance = onPreMint + onBuy - onSell;
                updateJo.Add("balance", balance.format());
            }
            
            //
            if (newB.transferFrom != zero)
            {
                var amt = newB.transferFrom.format();
                var amtOld = oldB.transferFrom.format();
                var amtOldTp = oldB.transferFromTp.format();
                if (fundPoolTempNotClearAllFlag)
                {
                    amtOldTp = decimal.Zero;
                }
                var amtUp = amtOld - amtOldTp + amt;
                var amtUpTp = amt;
                updateJo.Add("transferFrom", amtUp.format());
                updateJo.Add("transferFromTp", amtUpTp.format());
                updateFlag = true;
            }
            if (newB.transferTo != zero)
            {
                var amt = newB.transferTo.format();
                var amtOld = oldB.transferTo.format();
                var amtOldTp = oldB.transferToTp.format();
                if (fundPoolTempNotClearAllFlag)
                {
                    amtOldTp = decimal.Zero;
                }
                var amtUp = amtOld - amtOldTp + amt;
                var amtUpTp = amt;
                updateJo.Add("transferTo", amtUp.format());
                updateJo.Add("transferToTp", amtUpTp.format());
                updateFlag = true;
            }
            if (newB.onSetFdtIn != zero)
            {
                var amt = newB.onSetFdtIn.format();
                var amtOld = oldB.onSetFdtIn.format();
                var amtOldTp = oldB.onSetFdtInTp.format();
                if (fundPoolTempNotClearAllFlag)
                {
                    amtOldTp = decimal.Zero;
                }
                var amtUp = amtOld - amtOldTp + amt;
                var amtUpTp = amt;
                updateJo.Add("onSetFdtIn", amtUp.format());
                updateJo.Add("onSetFdtInTp", amtUpTp.format());
                updateFlag = true;
            }
            if (newB.onGetFdtOut != zero)
            {
                var amt = newB.onGetFdtOut.format();
                var amtOld = oldB.onGetFdtOut.format();
                var amtOldTp = oldB.onGetFdtOutTp.format();
                if (fundPoolTempNotClearAllFlag)
                {
                    amtOldTp = decimal.Zero;
                }
                var amtUp = amtOld - amtOldTp + amt;
                var amtUpTp = amt;
                updateJo.Add("onGetFdtOut", amtUp.format());
                updateJo.Add("onGetFdtOutTp", amtUpTp.format());
                updateFlag = true;
            }
            if (newB.onVote != zero)
            {
                var amt = newB.onVote.format();
                var amtOld = oldB.onVote.format();
                var amtOldTp = oldB.onVoteTp.format();
                if (fundPoolTempNotClearAllFlag)
                {
                    amtOldTp = decimal.Zero;
                }
                var amtUp = amtOld - amtOldTp + amt;
                var amtUpTp = amt;
                updateJo.Add("onVote", amtUp.format());
                updateJo.Add("onVoteTp", amtUpTp.format());
                updateFlag = true;
            }
            //
            if (updateFlag)
            {
                var updateBson = new BsonDocument { { "$set", updateJo } };
                mh.UpdateDecimal(daoConn.connStr, daoConn.connDB, projFinanceBalanceCol, updateBson, findStr);
            }
        }

        private Dictionary<string, JObject> getProjTokenName(string[] ids)
        {
            if (ids == null || ids.Count() == 0) return null;

            var findStr = MongoFieldHelper.toFilter(ids, "projId").ToString();
            var fieldStr = new JObject { { "projId",1 },{ "fundName", 1 }, { "tokenSymbol", 1 } }.ToString();
            var queryRes = mh.GetData(daoConn.connStr, daoConn.connDB, projFinanceCol, findStr, fieldStr);
            if (queryRes.Count == 0) return null;

            return queryRes.ToArray().ToDictionary(k=> k["projId"].ToString(), v=>new JObject { { "fundName",v["fundName"] },{ "tokenName", v["tokenSymbol"] } });
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
    
}
