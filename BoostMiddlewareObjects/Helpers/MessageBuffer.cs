using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace BoostMiddlewareObjects.Helpers
{
    public class MessageCache
    {
        public ConcurrentQueue<string> _msgCache = new ConcurrentQueue<string>();
        private int _cacheSize;

        public MessageCache(int cacheSize)
        {
            _cacheSize = cacheSize;
        }

        public void AddMessage(string msg, params object[] args)
        {
            _msgCache.Enqueue(String.Format(msg, args));
        }

        public void PrintMessagesToConsole()
        {
            while (_msgCache.Count > _cacheSize)
            {
                string dummy;
                _msgCache.TryDequeue(out dummy);
            }

            foreach (string msg in _msgCache.ToList())
            {
                Console.WriteLine(msg);
            }
        }
    }
}
