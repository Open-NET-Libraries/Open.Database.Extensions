using NSubstitute;
using System;
using System.Data;
using Xunit;

namespace Open.Database.Extensions.Core.Tests
{
    public class Basic
    {
        [Fact]
        public void ExpressiveCommandValidation()
        {
            var factory = DbConnectionFactory.Create(() =>
            {
                var conn = Substitute.For<IDbConnection>();
                return conn;
            });

            Assert.Throws<ArgumentNullException>(() => factory.Command(null));
            Assert.Throws<ArgumentException>(() => factory.Command(string.Empty));
        }

    }
}
