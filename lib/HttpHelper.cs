using System.Net;

namespace NEL_FutureDao_BT.lib
{
    class HttpHelper
    {
        public static string HttpPost(string url, byte[] data)
        {
            WebClient wc = new WebClient();
            wc.Headers["content-type"] = "text/plain;charset=UTF-8";
            byte[] retdata = wc.UploadData(url, "POST", data);
            return System.Text.Encoding.UTF8.GetString(retdata);
        }
    }
}
