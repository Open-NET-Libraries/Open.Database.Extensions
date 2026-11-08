namespace Open.Database.Extensions;

/// <summary>
/// A connection factory implementation backed by a factory function. Use <see cref="Create{TConnection}(Func{TConnection})"/> or the implicit conversion from <see cref="Func{TResult}"/> to build one.
/// </summary>
/// <remarks>
/// Every call to <see cref="Create()"/> invokes the function and returns whatever it returns; the factory itself holds no connections and keeps no state. The caller of <see cref="Create()"/> owns the connection and disposes it.
/// </remarks>
/// <seealso cref="DbConnectionFactory{TConnection}"/>
/// <seealso cref="IDbConnectionFactory"/>
public class DbConnectionFactory : IDbConnectionFactory
{
	/// <summary>
	/// Constructs a <see cref="DbConnectionFactory"/> from a factory function.
	/// </summary>
	/// <param name="factory">The function that creates a new connection each time it is called.</param>
	/// <exception cref="ArgumentNullException"><paramref name="factory"/> is <see langword="null"/>.</exception>
	protected DbConnectionFactory(Func<IDbConnection> factory)
	{
		_factory = factory ?? throw new ArgumentNullException(nameof(factory));
		Contract.EndContractBlock();
	}

	readonly Func<IDbConnection> _factory;

	/// <summary>
	/// Creates a new connection by invoking the factory function.
	/// </summary>
	/// <returns>The connection returned by the factory function. The caller owns it and is responsible for disposing it.</returns>
	/// <remarks>
	/// Exceptions thrown by the factory function propagate unchanged.
	/// </remarks>
	public IDbConnection Create() => _factory();

	/// <summary>
	/// Creates a strongly typed <see cref="DbConnectionFactory{TConnection}"/> from a factory function.
	/// </summary>
	/// <typeparam name="TConnection">The connection type the function creates.</typeparam>
	/// <param name="factory">The function that creates a new connection each time it is called.</param>
	/// <returns>A <see cref="DbConnectionFactory{TConnection}"/> that invokes <paramref name="factory"/> for every connection.</returns>
	/// <exception cref="ArgumentNullException"><paramref name="factory"/> is <see langword="null"/>.</exception>
	public static DbConnectionFactory<TConnection> Create<TConnection>(Func<TConnection> factory)
		where TConnection : IDbConnection
		=> new(factory);

	/// <summary>
	/// Implicitly converts a connection factory function to a connection factory instance.
	/// </summary>
	/// <param name="factory">The function that creates a new connection each time it is called.</param>
	/// <exception cref="ArgumentNullException"><paramref name="factory"/> is <see langword="null"/>.</exception>
	public static implicit operator DbConnectionFactory(Func<IDbConnection> factory)
		=> Create(factory);
}

/// <summary>
/// A strongly typed connection factory implementation backed by a factory function.
/// </summary>
/// <typeparam name="TConnection">The connection type.</typeparam>
/// <remarks>
/// Every call to <see cref="Create()"/> invokes the function. The caller owns the returned connection and disposes it.
/// </remarks>
/// <seealso cref="DbConnectionFactory"/>
/// <seealso cref="IDbConnectionFactory{TConnection}"/>
public class DbConnectionFactory<TConnection>
	: DbConnectionFactory, IDbConnectionFactory<TConnection>
	where TConnection : IDbConnection
{
	readonly Func<TConnection> _factory;

	/// <summary>
	/// Constructs a <see cref="DbConnectionFactory{TConnection}"/> from a factory function.
	/// </summary>
	/// <inheritdoc cref="DbConnectionFactory.DbConnectionFactory(Func{IDbConnection})"/>
	public DbConnectionFactory(Func<TConnection> factory)
		: base(() => factory())
	{
		_factory = factory ?? throw new ArgumentNullException(nameof(factory));
		Contract.EndContractBlock();
	}

	IDbConnection IDbConnectionFactory.Create() => Create();

	/// <returns>The <typeparamref name="TConnection"/> returned by the factory function. The caller owns it and is responsible for disposing it.</returns>
	/// <inheritdoc cref="DbConnectionFactory.Create()"/>
	public new TConnection Create() => _factory();

	/// <summary>
	/// Implicitly converts a typed connection factory function to a typed connection factory instance.
	/// </summary>
	/// <param name="factory">The function that creates a new connection each time it is called.</param>
	/// <exception cref="ArgumentNullException"><paramref name="factory"/> is <see langword="null"/>.</exception>
	public static implicit operator DbConnectionFactory<TConnection>(Func<TConnection> factory)
		=> Create(factory);
}
