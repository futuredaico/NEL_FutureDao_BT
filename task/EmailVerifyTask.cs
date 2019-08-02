using NEL.NNS.lib;
using NEL_FutureDao_BT.core;
using NEL_FutureDao_BT.lib;
using Newtonsoft.Json.Linq;
using System.Linq;

namespace NEL_FutureDao_BT.task
{
    class EmailVerifyTask : AbsTask
    {
        private MongoDBHelper mh = new MongoDBHelper();
        private DbConnInfo daoConn;
        private string userInfoCol;
        private string projInfoCol;
        private string projTeamInfoCol;
        private int batchSize;
        private int batchInterval;
        private string emailRegisterUrl;
        private string emailResetPswdUrl;
        private string emailChangeMailUrl;
        private string emailInviteUserUrl;
        private string emailRegisterBody;
        private string emailResetPswdBody;
        private string emailChangeMailBody;
        private string emailInviteUserBody;
        private EmailHelper eh;

        public EmailVerifyTask(string name) : base(name) { }

        public override void initConfig(JObject config)
        {
            JToken cfg = config["TaskList"].Where(p => p["taskName"].ToString() == name() && p["taskNet"].ToString() == networkType()).ToArray()[0]["taskInfo"];
            userInfoCol = cfg["userInfoCol"].ToString();
            projInfoCol = cfg["projInfoCol"].ToString();
            projTeamInfoCol = cfg["projTeamInfoCol"].ToString();
            batchSize = int.Parse(cfg["batchSize"].ToString());
            batchInterval = int.Parse(cfg["batchInterval"].ToString());
            emailRegisterUrl = cfg["verifyModule"]["registerUrl"].ToString();
            emailResetPswdUrl = cfg["verifyModule"]["resetPswdUrl"].ToString();
            emailChangeMailUrl = cfg["verifyModule"]["changeEmailUrl"].ToString();
            emailInviteUserUrl = cfg["verifyModule"]["inviteUserUrl"].ToString();
            emailRegisterBody = cfg["verifyModule"]["registerBody"].ToString();
            emailResetPswdBody = cfg["verifyModule"]["resetPswdBody"].ToString();
            emailChangeMailBody = cfg["verifyModule"]["changeEmailBody"].ToString();
            emailInviteUserBody = cfg["verifyModule"]["inviteUserBody"].ToString();

            eh = new EmailHelper { config = Config.emailInfo };
            daoConn = Config.daoDbConnInfo;
            hasInitSuccess = true;
        }

        public override void process()
        {
            // userInfoCol
            string findStr = new string[] {
                EmailState.sendBeforeState,
                EmailState.sendBeforeStateAtResetPassword,
                EmailState.sendBeforeStateAtChangeEmail
            }.toFilter("emailVerifyState").ToString();
            string sortStr = "{'time':1}";
            string fieldStr = "{'username':1,'email':1,'emailVerifyState':1}";
            while (true)
            {
                var queryRes = mh.GetDataPages(daoConn.connStr, daoConn.connDB, userInfoCol, findStr, sortStr, 0, batchSize, fieldStr);
                if (queryRes.Count == 0) break;

                foreach (var item in queryRes)
                {
                    handle(item, userInfoCol);
                }
            }


            // projTeamInfoCol
            findStr = new string[] {
                EmailState.sendBeforeStateAtInvited
            }.toFilter("emailVerifyState").ToString();
            fieldStr = "{'projId':1,'username':1,'email':1,'emailVerifyState':1}";
            while (true)
            {
                var queryRes = mh.GetDataPages(daoConn.connStr, daoConn.connDB, projTeamInfoCol, findStr, sortStr, 0, batchSize, fieldStr);
                if (queryRes.Count == 0) break;

                foreach (var item in queryRes)
                {
                    handle(item, projTeamInfoCol, true);
                }
            }

            Log(batchInterval);
            //
            if (hasCreateIndex) return;
            mh.setIndex(daoConn.connStr, daoConn.connDB, userInfoCol, "{'userId':1}", "i_userId");
            mh.setIndex(daoConn.connStr, daoConn.connDB, userInfoCol, "{'username':1}", "i_username");
            mh.setIndex(daoConn.connStr, daoConn.connDB, userInfoCol, "{'username':1,'email':1,'password':1}", "i_username_email_pswd");
            mh.setIndex(daoConn.connStr, daoConn.connDB, userInfoCol, "{'email':1}", "i_email");
            mh.setIndex(daoConn.connStr, daoConn.connDB, userInfoCol, "{'emailVerifyState':1}", "i_emailVerifyState");

            mh.setIndex(daoConn.connStr, daoConn.connDB, projInfoCol, "{'projId':1}", "i_projId");
            mh.setIndex(daoConn.connStr, daoConn.connDB, projInfoCol, "{'projName':1}", "i_projName");
            mh.setIndex(daoConn.connStr, daoConn.connDB, projInfoCol, "{'projTitle':1}", "i_projTitle");

            mh.setIndex(daoConn.connStr, daoConn.connDB, projTeamInfoCol, "{'projId':1}", "i_projId");
            mh.setIndex(daoConn.connStr, daoConn.connDB, projTeamInfoCol, "{'projId':1,'userId':1}", "i_projId_userId");
            mh.setIndex(daoConn.connStr, daoConn.connDB, projTeamInfoCol, "{'emailVerifyState':1}", "i_emailVerifyState");
            hasCreateIndex = true;
        }
        
        private void handle(JToken jt, string coll, bool hasProjId=false)
        {
            string username = jt["username"].ToString();
            string email = jt["email"].ToString();
            string emailVerifyState = jt["emailVerifyState"].ToString();
            string vcode = UIDHelper.generateVerifyCode();
            string projId = hasProjId ? jt["projId"].ToString() : "";

            if (toMessage(emailVerifyState, username, email, projId, vcode, out string message, out string hasSendState))
            {
                eh.send(message, email, true);
                var findStr = new JObject { { "email", email } }.ToString();
                var updateStr = new JObject { { "$set", new JObject { { "emailVerifyCode", vcode }, { "emailVerifyState", hasSendState }, { "lastUpdateTime", TimeHelper.GetTimeStamp() } } } }.ToString();
                mh.UpdateData(daoConn.connStr, daoConn.connDB, coll, updateStr, findStr);
            }
        }

        public bool toMessage(string type, string username, string email, string projId, string uid, out string message, out string hasSendState)
        {
            string prefix = "";
            string body = "";
            if (type == EmailState.sendBeforeState)
            {
                prefix = emailRegisterUrl;
                body = emailRegisterBody;
                hasSendState = EmailState.sendAfterState;
            }
            else if (type == EmailState.sendBeforeStateAtResetPassword)
            {
                prefix = emailResetPswdUrl;
                body = emailResetPswdBody;
                hasSendState = EmailState.sendAfterStateAtResetPassword;
            }
            else if (type == EmailState.sendBeforeStateAtChangeEmail)
            {
                prefix = emailChangeMailUrl;
                body = emailChangeMailBody;
                hasSendState = EmailState.sendAfterStateAtChangeEmail;
            }
            else if(type == EmailState.sendBeforeStateAtInvited)
            {
                prefix = emailInviteUserUrl;
                body = emailInviteUserBody;
                hasSendState = EmailState.sendAfterStateAtInvited;
            }
            else
            {
                message = "";
                hasSendState = "";
                return false;
            }
            if(projId == "")
            {
                message = string.Format(body, string.Format(prefix, username, email, uid));
            } else
            {
                message = string.Format(body, string.Format(prefix, username, email, projId, uid));
            }
            //message = string.Format(body, string.Format(prefix, username, email, uid));
            return true;
        }
    }


    class EmailState
    {
        public const string sendBeforeState = "10101";
        public const string sendAfterState = "10102";
        public const string hasVerify = "10103";
        public const string hasVerifyFailed = "10104";
        public const string sendBeforeStateAtResetPassword = "10105";
        public const string sendAfterStateAtResetPassword = "10106";
        public const string hasVerifyAtResetPassword = "10107";
        public const string hasVerifyAtResetPasswordFailed = "10108";
        public const string sendBeforeStateAtChangeEmail = "10109";
        public const string sendAfterStateAtChangeEmail = "10110";
        public const string hasVerifyAtChangeEmail = "10111";
        public const string hasVerifyAtChangeEmailFailed = "10112";
        public const string sendBeforeStateAtInvited = "10113";
        public const string sendAfterStateAtInvited = "10114";
        public const string hasVerifyAtInvitedYes = "10115";
        public const string hasVerifyAtInvitedNot = "10116";
    }
}
