using System;
using System.Collections.Generic;
using System.Data;
using System.Diagnostics;
using System.Linq;
using System.Threading.Tasks;
using EntityFrameworkCore.PostgreSQL.Copy;
using Microsoft.EntityFrameworkCore;

namespace TestApp
{
    class Program
    {
        static async Task Main(string[] args)
        {
            await Exec();
        }

        private static async Task Exec()
        {
            Console.WriteLine("----------------------" +
                              "------------");
            var stopwatch = Stopwatch.StartNew();

            using (var db = new MyContext())
            {
                using (var transaction = db.Database.BeginTransaction(IsolationLevel.ReadCommitted))
                {
//                    db.Database.OpenConnection();

                    var mergeOperation = new MergeOperation<Blog>(db);
                    mergeOperation.WithData(() => GetBlogToInsert(100500));
                    mergeOperation.OnConflictDoUpdate(
                        (Blog b) => new {b.BlogId},
                        (target, excluded) => new Blog
                        {
                            Url = excluded.Url,
//                            Url = "TEST",
                        });

                    await mergeOperation.ExecuteAsync();

                    transaction.Commit();
                }
            }

            stopwatch.Stop();

            Console.WriteLine("-------------- DONE --------------");
            Console.WriteLine(stopwatch.Elapsed);
        }

        private static IEnumerable<Blog> GetBlogToInsert(int count)
        {
            foreach (var id in Enumerable.Range(1, count))
            {
                yield return new Blog
                {
                    BlogId = id,
                    Url = "Url " + id
                };
            }
        }
    }
}