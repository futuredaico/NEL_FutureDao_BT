using System;
using System.Collections.Generic;
using System.Text;

namespace NEL_FutureDao_BT.lib
{
    class UIDHelper
    {
        private static string source = "01234567890abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ";
        public static string generateVerifyCode()
        {
            int len = source.Length;
            StringBuilder sb = new StringBuilder();
            for (int i = 0; i < 8; ++i)
            {
                sb.Append(source[new Random().Next(len)]);
            }
            return sb.ToString();
        }
    }
}
