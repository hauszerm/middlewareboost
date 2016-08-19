using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Intratec.Common.Helpers.Extensions
{
    public static class LinqExtensions
    {
        public static IEnumerable<TSource> FromHierarchy<TSource>(
            this TSource source,
            Func<TSource, TSource> nextItem,
            Func<TSource, bool> canContinue)
        {
            //code from http://stackoverflow.com/questions/9314172/getting-all-messages-from-innerexceptions

            for (var current = source; canContinue(current); current = nextItem(current))
            {
                yield return current;
            }
        }

        /// <summary>
        /// Returns a list of objects where the next object is linked by the previous object with the property specified in <paramref name="nextitem"/>.
        /// </summary>
        /// <typeparam name="TSource"></typeparam>
        /// <param name="source"></param>
        /// <param name="nextItem"></param>
        /// <returns></returns>
        public static IEnumerable<TSource> FromHierarchy<TSource>(
                this TSource source,
                Func<TSource, TSource> nextItem)
                where TSource : class
        {
            //code from http://stackoverflow.com/questions/9314172/getting-all-messages-from-innerexceptions

            return FromHierarchy(source, nextItem, s => s != null);
        }
    }
}
