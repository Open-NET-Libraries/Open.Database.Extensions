using System;
using System.Data;
using System.Diagnostics.Contracts;

namespace Open.Database.Extensions
{
	/// <summary>
	/// DbConnection factory implementation that accepts a factory function.
	/// </summary>
	public class DbConnectionFactory : IDbConnectionFactory
	{
		/// <summary>
		/// Constructs a DbConnectionFactory.
		/// </summary>
		/// <param name="factory"></param>
		protected DbConnectionFactory(Func<IDbConnection> factory)
		{
			_factory = factory ?? throw new ArgumentNullException(nameof(factory));
			Contract.EndContractBlock();
		}

		readonly Func<IDbConnection> _factory;

		/// <summary>
		/// Creates a connection of from the underlying factory function. 
		/// </summary>
		public IDbConnection Create() => _factory();

		/// <summary>
		/// Creates a Non-Generic DbConnectionFactory.
		/// </summary>
		/// <param name="factory">The factory function.</param>
		/// <returns>A Non-Generic DbConnectionFactory</returns>
		public static DbConnectionFactory<TConnection> Create<TConnection>(Func<TConnection> factory)
			where TConnection : IDbConnection
			=> new DbConnectionFactory<TConnection>(factory);

		/// <summary>
		/// Implicitly converts a connection factory function to a connection factory instance.
		/// </summary>
		/// <param name="factory"></param>
		[System.Diagnostics.CodeAnalysis.SuppressMessage("Usage", "CA2225:Operator overloads have named alternates", Justification = "Create method is availble.")]
		public static implicit operator DbConnectionFactory(Func<IDbConnection> factory)
			=> Create(factory);
	}

	/// <summary>
	/// Generic connection factory implementation that accepts a factory function.
	/// </summary>
	/// <typeparam name="TConnection">The connection type.</typeparam>
	public class DbConnectionFactory<TConnection> : DbConnectionFactory, IDbConnectionFactory<TConnection>
		where TConnection : IDbConnection
	{
		readonly Func<TConnection> _factory;

		/// <summary>
		/// Constructs a DbConnectionFactory.
		/// </summary>
		/// <param name="factory">The factory function.</param>
		public DbConnectionFactory(Func<TConnection> factory)
			: base(() => factory())
		{
			_factory = factory ?? throw new ArgumentNullException(nameof(factory));
			Contract.EndContractBlock();
		}

		IDbConnection IDbConnectionFactory.Create() => Create();

		/// <summary>
		/// Creates a connection of from the underlying factory function. 
		/// </summary>
		public new TConnection Create() => _factory();


		/// <summary>
		/// Implicitly converts a connection factory function to a genetic-typed connection factory instance.
		/// </summary>
		/// <param name="factory"></param>
		[System.Diagnostics.CodeAnalysis.SuppressMessage("Usage", "CA2225:Operator overloads have named alternates", Justification = "Create method is availble.")]
		public static implicit operator DbConnectionFactory<TConnection>(Func<TConnection> factory)
			=> Create(factory);
	}
}
