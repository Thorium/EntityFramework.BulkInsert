using System;
using System.Collections.Generic;
using System.Data.Entity;
using System.Data.SqlClient;
using System.Linq;
using System.Transactions;
using System.Xml.Serialization;
using EntityFramework.BulkInsert.Extensions;
using EntityFramework.BulkInsert.Providers;
using EntityFramework.BulkInsert.Test.CodeFirst.Domain;
using EntityFramework.BulkInsert.Test.Domain;
using NUnit.Framework;

#if EF6
using Calculator.Data;
using Calculator.Entities;
using System.Data.SqlClient;
#endif

namespace EntityFramework.BulkInsert.Test.CodeFirst.BulkInsert
{
    [TestFixture]
    public abstract class BulkInsertTestBase<T> : TestBase where T : IEfBulkInsertProvider, new()
    {
        public override void Setup()
        {
            ProviderFactory.Register<T>(ProviderConnectionType);
            base.Setup();
        }

        protected abstract string ProviderConnectionType { get; }

        [Test]
        public void BulkInsertToTableInNonDefaultSchema()
        {
            using (var ctx = GetContext())
            {
                var foos = new[] {new Foo {Bar = "bar"}};
                ctx.BulkInsert(foos);
            }
        }

        [Test]
        public void BulkInsertWithIdentityInsertOn()
        {
            using (var ctx = GetContext())
            {
                var newGuid = Guid.NewGuid();
                var testUser = new TestUser { Id = newGuid, CreatedAt = DateTime.Now };
                var users = new[] { testUser };
                ctx.BulkInsert(users, SqlBulkCopyOptions.KeepIdentity);

                var lastinsert = ctx.Users.OrderByDescending(x => x.CreatedAt).FirstOrDefault();
                Assert.AreEqual(testUser.Id, lastinsert.Id);
            }
        }

        [Test]
        public void BulkInsertWithIdentityInsertOff()
        {
            using (var ctx = GetContext())
            {
                var newGuid = Guid.NewGuid();
                var testUser = new TestUser { Id = newGuid, CreatedAt = DateTime.Now };
                var users = new[] { testUser };
                ctx.BulkInsert(users);

                var lastinsert = ctx.Users.OrderByDescending(x => x.CreatedAt).FirstOrDefault();
                Assert.AreNotEqual(testUser.Id, lastinsert.Id);
            }
        }

        [Test]
        public void BulkInsertTableWithComputedColumns()
        {
            using (var ctx = GetContext())
            {
                var rand = new Random();
                var x = rand.Next(100);
                var y = rand.Next(100);
                var foos = new[] { new Foo { Bar = "bar", X = x, Y = y} };
                ctx.BulkInsert(foos);

                var foo = ctx.Foos.OrderByDescending(f => f.Id).First();
                Assert.AreEqual(x, foo.X);
                Assert.AreEqual(y, foo.Y);
                Assert.AreEqual(x*y, foo.Z);
            }
        }

        [Test]
        public void MixedTransactionsRollback()
        {
            using (var ctx = GetContext())
            {
                var count = ctx.Pages.Count();

                try
                {
                    using (var transactionScope = new TransactionScope())
                    {
                        ctx.Pages.Add(new Page {Title = "test", Content = "Test"});

                        ctx.BulkInsert(CreatePages(10));

                        ctx.SaveChanges();
                        transactionScope.Complete();
                    }

                    Assert.Fail("Should throw an exception");
                }
                catch
                {
                    var countAfter = ctx.Pages.Count();
                    Assert.AreEqual(count, countAfter);
                }
            }
        }

#if EF6
        [Test]
        public void Issue1344Test()
        {
            using (var ctx = new AccrualContext())
            {
                var post = new Post { Oid = Guid.NewGuid(), StartDate = DateTime.Now, EndDate = DateTime.Now };
                var posts = new[] { post };
                ctx.BulkInsert(posts, SqlBulkCopyOptions.KeepIdentity);
            }
        }
#endif

        [Test]
        public void MixedTransactionsCommit()
        {
            using (var ctx = GetContext())
            {
                var count = ctx.Pages.Count();

                using (var transactionScope = new TransactionScope())
                {
                    ctx.Pages.Add(new Page {Title = "test", Content = "Test", CreatedAt = DateTime.Now });

                    ctx.BulkInsert(CreatePages(10));

                    ctx.SaveChanges();
                    transactionScope.Complete();
                }

                var countAfter = ctx.Pages.Count();
                Assert.AreEqual(count + 11, countAfter);
               
            }
        }

        [Test]
        public void SimpleType()
        {
            using (var ctx = GetContext())
            {
                const int itemsCount = 20;
                var pages = CreatePages(itemsCount);
                RunBulkInsert(ctx, pages, itemsCount);
            }
        }

        [Test]
        public void ComplexTypes()
        {
            using (var ctx = GetContext())
            {
                const int itemsCount = 1;
                var users = CreateUsers(itemsCount).ToArray();
                RunBulkInsert(ctx, users, itemsCount);

                var createdUser = users[0];
                var userInDb = ctx.Users.OrderByDescending(x => x.CreatedAt).First();

                var serializer = new XmlSerializer(typeof (TestUser));
                serializer.Serialize(Console.Out, userInDb);

                Assert.AreEqual(createdUser.FirstName, userInDb.FirstName);
                Assert.AreEqual(createdUser.LastName, userInDb.LastName);
                Assert.AreEqual(createdUser.Contact.PhoneNumber, userInDb.Contact.PhoneNumber);
                Assert.AreEqual(createdUser.Contact.Address.City, userInDb.Contact.Address.City);
                Assert.AreEqual(createdUser.Contact.Address.Country, userInDb.Contact.Address.Country);
                Assert.AreEqual(createdUser.Contact.Address.County, userInDb.Contact.Address.County);
                Assert.AreEqual(createdUser.Contact.Address.PostalCode, userInDb.Contact.Address.PostalCode);
                Assert.AreEqual(createdUser.Contact.Address.StreetAddress, userInDb.Contact.Address.StreetAddress);

                Assert.AreEqual(createdUser.CreatedAt.ToString(), userInDb.CreatedAt.ToString());
            }
        }

        [Test]
        public void TPH_BaseType()
        {
            using (var ctx = GetContext())
            {
                var contracts = new List<ContractBase>();

                var c1 = new ContractFixed
                {
                    AvpContractNr = "c_FIX", 
                    PackageFixedId = 1, 
                    PricesJson = "{}",
                    MeteringPointId = 2,
                    ClientId = 5,
                };
                contracts.Add(c1);

                var c2 = new ContractStock {AvpContractNr = "c_STX", Margin = 0.001m, PackageStockId = 2};
                contracts.Add(c2);

                var k1 = new ContractKomb1 {AvpContractNr = "c_K1", PackageKomb1Id = 3};
                contracts.Add(k1);

                var k2 = new ContractKomb2 {AvpContractNr = "c_K2", PackageKomb2Id = 3, Part1Margin = 0.1m};
                contracts.Add(k2);

                ctx.BulkInsert(contracts);

                var c1db = ctx.FixedContracts.OrderByDescending(x => x.Id).First();

                Assert.AreEqual(c1.AvpContractNr, c1db.AvpContractNr);
                Assert.AreEqual(c1.PackageFixedId, c1db.PackageFixedId);
                Assert.AreEqual(c1.PricesJson, c1db.PricesJson);
                Assert.AreEqual(c1.MeteringPointId, c1db.MeteringPointId);
                Assert.AreEqual(c1.ClientId, c1db.ClientId);
                Assert.AreEqual(c1.ContractNr, c1db.ContractNr);
                Assert.AreEqual(c1.ContractSignedAt, c1db.ContractSignedAt);

                var k1db = ctx.K1Contracts.OrderByDescending(x => x.Id).First();

                Assert.AreEqual(k1.AvpContractNr, k1db.AvpContractNr);
                Assert.AreEqual(k1.PackageKomb1Id, k1db.PackageKomb1Id);
                Assert.AreEqual(k1.AvpContractNr, k1db.AvpContractNr);
                Assert.AreEqual(k1.MeteringPointId, k1db.MeteringPointId);
                Assert.AreEqual(k1.ClientId, k1db.ClientId);
                Assert.AreEqual(k1.ContractNr, k1db.ContractNr);
                Assert.AreEqual(k1.ContractSignedAt, k1db.ContractSignedAt);
            }
        }
        
        [Test]
        public void TPH_SubType_First()
        {
            using (var ctx = GetContext())
            {
                var contracts = new List<ContractFixed>();

                var c1 = new ContractFixed
                {
                    AvpContractNr = "c_FIX", 
                    PackageFixedId = 1, 
                    PricesJson = "{}",
                    MeteringPointId = 2,
                    ClientId = 5,
                };
                contracts.Add(c1);

                ctx.BulkInsert(contracts);

                var c1db = ctx.FixedContracts.OrderByDescending(x => x.Id).First();

                Assert.AreEqual(c1.AvpContractNr, c1db.AvpContractNr);
                Assert.AreEqual(c1.PackageFixedId, c1db.PackageFixedId);
                Assert.AreEqual(c1.PricesJson, c1db.PricesJson);
                Assert.AreEqual(c1.MeteringPointId, c1db.MeteringPointId);
                Assert.AreEqual(c1.ClientId, c1db.ClientId);
                Assert.AreEqual(c1.ContractNr, c1db.ContractNr);
                Assert.AreEqual(c1.ContractSignedAt, c1db.ContractSignedAt);
            }
        }

        [Test]
        public void TPH_SubType_NotFirst()
        {
            using (var ctx = GetContext())
            {
                var contracts = new List<ContractKomb1>();

                var k1 = new ContractKomb1 { AvpContractNr = "c_K1", PackageKomb1Id = 3 };
                contracts.Add(k1);

                ctx.BulkInsert(contracts);

                var k1db = ctx.K1Contracts.OrderByDescending(x => x.Id).First();

                Assert.AreEqual(k1.AvpContractNr, k1db.AvpContractNr);
                Assert.AreEqual(k1.PackageKomb1Id, k1db.PackageKomb1Id);
                Assert.AreEqual(k1.AvpContractNr, k1db.AvpContractNr);
                Assert.AreEqual(k1.MeteringPointId, k1db.MeteringPointId);
                Assert.AreEqual(k1.ClientId, k1db.ClientId);
                Assert.AreEqual(k1.ContractNr, k1db.ContractNr);
                Assert.AreEqual(k1.ContractSignedAt, k1db.ContractSignedAt);
            }
        }
        
        [Test]
        public void TPT_SubType_First()
        {
            using (var ctx = GetContext())
            {
                var employees = new List<ManagerTPT>();
                
                var manager = new ManagerTPT { JobTitle = "Manager", Name = "Bar", Rank = "low" };
                employees.Add(manager);

                ctx.BulkInsert(employees);
                
                var dbManager = ctx.ManagerTpts.OrderByDescending(x => x.Id).First();

                Assert.AreEqual(manager.JobTitle, dbManager.JobTitle);
                Assert.AreEqual(manager.Name, dbManager.Name);
                Assert.AreEqual(manager.Rank, dbManager.Rank);
            }
        }

        [Test]
        public void TPT_SubType_NotFirst()
        {
            using (var ctx = GetContext())
            {
                var employees = new List<WorkerTPT>();

                var worker = new WorkerTPT { JobTitle = "Worker", Name = "Foo", Boss = new ManagerTPT { Id = 1, Name = "The boss", Rank = "High", JobTitle = "The manager" }};
                employees.Add(worker);
                
                ctx.BulkInsert(employees);

                var dbWorker = ctx.WorkerTpts.OrderByDescending(x => x.Id).First();

                Assert.AreEqual(worker.JobTitle, dbWorker.JobTitle);
                Assert.AreEqual(worker.Name, dbWorker.Name);
            }
        }
    }

#if EF4 || EF5
    public static class Ef4Compatibiliy
    {
        public static void AddRange<T>(this DbSet<T> set, IEnumerable<T> items) where T : class
        {
            foreach (var item in items)
            {
                set.Add(item);
            }
        }
    }
#endif
}
