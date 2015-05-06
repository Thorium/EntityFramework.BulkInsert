using System;
using System.Collections.Generic;
using EntityFramework.BulkInsert.Helpers;
using EntityFramework.BulkInsert.Test.CodeFirst;
using EntityFramework.BulkInsert.Test.Domain;
using EntityFramework.BulkInsert.Test.Domain.ComplexTypes;
using EntityFramework.MappingAPI;
using EntityFramework.MappingAPI.Extensions;
using NUnit.Framework;
using TestContext = EntityFramework.BulkInsert.Test.CodeFirst.TestContext;

namespace EntityFramework.BulkInsert.Test
{
    [TestFixture]
    public class MappedDataReaderTest : TestBase
    {
        [Test]
        public void SimpleTableReader()
        {
            using (var ctx = new TestContext())
            {
                var tableMapping = ctx.Db<Page>();

                var tableMappings = new Dictionary<Type, IEntityMap>
                {
                    {typeof (Page), tableMapping}
                };

                using (var reader = new MappedDataReader<Page>(new[] {new Page { Title = "test"}}, tableMappings))
                {
                    Assert.AreEqual(6, reader.FieldCount);
                }
            }
        }

        [Test]
        public void ComplexTypeReader()
        {
            var user = new TestUser
            {
                Contact = new Contact { Address = new Address { City = "Tallinn", Country = "Estonia"}, PhoneNumber = "1234567"},
                FirstName = "Max",
                LastName = "Lego",
                Id = Guid.NewGuid()
            };
            var emptyUser = new TestUser();

            using (var ctx = new TestContext())
            {
                var tableMapping = ctx.Db<TestUser>();

                var tableMappings = new Dictionary<Type, IEntityMap>
                {
                    {typeof (TestUser), tableMapping}
                };
                using (var reader = new MappedDataReader<TestUser>(new[] { user, emptyUser }, tableMappings))
                {
                    Assert.AreEqual(9, reader.FieldCount);
                    
                    while (reader.Read())
                    {
                        for (int i = 0; i < reader.FieldCount; ++i)
                        {
                            Console.WriteLine("{0}: {1}", i, reader.GetValue(i));
                        }
                    }
                }
            }
        }
    }
}