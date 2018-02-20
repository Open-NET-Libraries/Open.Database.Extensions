using System;
using System.Data;
using System.Data.Common;

namespace Open.Database.Extensions
{
    /// <summary>
    /// Generic connection factory implementation that accepts a factory function.
    /// </summary>
    /// <typeparam name="TConnection"></typeparam>
    public class DbConnectionFactory<TConnection> : IDbConnectionFactory<TConnection>
        where TConnection:DbConnection
    {
        readonly Func<TConnection> _factory;

        /// <summary>
        /// Constructs a DbConnectionFactory.
        /// </summary>
        /// <param name="factory"></param>
        public DbConnectionFactory(Func<TConnection> factory)
        {
            _factory = factory ?? throw new ArgumentNullException(nameof(factory));
        }

        /// <summary>
        /// Creates a connection of from the underlying factory function. 
        /// </summary>
        public virtual TConnection Create() => _factory();

        IDbConnection IDbConnectionFactory.Create() => Create();
    }

    /// <summary>
    /// DbConnection factory implementation that accepts a factory function.
    /// </summary>
    public class DbConnectionFactory : DbConnectionFactory<DbConnection>
    {
        /// <summary>
        /// Constructs a DbConnectionFactory.
        /// </summary>
        /// <param name="factory"></param>
        public DbConnectionFactory(Func<DbConnection> factory) : base(factory)
        {
        }
    }
}
