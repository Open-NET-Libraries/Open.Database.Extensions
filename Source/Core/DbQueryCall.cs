namespace Open.Database.Extensions;

/// <summary>
/// A <see cref="DbQuery"/> bound to a source with some values added.
/// Immutable: each <c>AddParam</c> returns a new one and leaves this one as
/// it was, so a call can be kept and branched.
/// </summary>
/// <remarks>
/// Map it with <c>Map&lt;T&gt;()</c>, which is the same extension every command
/// uses, then run a terminal. Nothing touches the database before that.
/// </remarks>
public abstract class DbQueryCall : IExecuteReaderAsync
{
	private protected DbQueryCall() { }

	internal abstract DbQuery Definition { get; }

	internal abstract object SourceCore { get; }

	// Binds this node's parameters after its parent's, in the order added.
	internal abstract void BindAll(DbCommand command);

	/// <summary>
	/// The connection, transaction, pool or factory the query is bound to.
	/// </summary>
	public object Source => SourceCore;

	/// <inheritdoc cref="DbQueryUsing.AddParams{T}(T)"/>
	public DbQueryCall AddParams<T>(T values)
		where T : notnull
		=> new Node<T>(this, null, values);

	/// <inheritdoc cref="DbQueryUsing.AddParam{T}(string, T)"/>
	public DbQueryCall AddParam<T>(string name, T value)
		=> new Node<T>(this, CheckedName(name), value);

	/// <inheritdoc cref="DbQueryUsing.AddParam(string, string?)"/>
	public DbQueryCall AddParam(string name, string? value)
		=> new Node<string?>(this, CheckedName(name), value);

	/// <inheritdoc cref="DbQueryUsing.AddParam{T}(T, string?)"/>
	[OverloadResolutionPriority(-1)]
	public DbQueryCall AddParam<T>(
		T value,
		[CallerArgumentExpression(nameof(value))] string? expression = null)
		=> new Node<T>(this, Derive(expression), value);

	internal static DbQueryCall NewRoot<T>(
		DbQuery definition,
		object source,
		string? name,
		T value)
		=> new Root<T>(definition, source, name, value);

	internal static string CheckedName(string name)
	{
		if (name is null) throw new ArgumentNullException(nameof(name));
		if (string.IsNullOrWhiteSpace(name))
		{
			throw new ArgumentException(
				"Parameter names cannot be empty or white space.",
				nameof(name));
		}

		return name;
	}

	// The name of AddParam(value): the last member of the argument as
	// written, which the compiler supplies. The facts of an expression are
	// worked out once per expression text.
	[SuppressMessage(
		"Usage",
		"CA2208:Instantiate argument exceptions correctly")]
	internal static string Derive(string? expression)
		=> ParameterNames.Describe(expression) is { } described
			? described.LastMember
			: throw (expression is null
				? new ArgumentException(
					"The compiler did not supply the argument's text (deriving a name needs C# 10 or newer). Pass the name explicitly: AddParam(\"name\", value).",
					"value")
				: new ArgumentException(
					$"Cannot derive a parameter name from the argument '{expression}'. Pass a variable or a chain of members such as filter.AuthorId, or name the parameter: AddParam(\"name\", value).",
					"value"));

	static void BindValue<T>(DbCommand command, string? name, T value)
	{
		if (name is not null)
		{
			Add(command, name, value, typeof(T));
			return;
		}

		// The members of the values, found once per type.
		if (typeof(T) == typeof(object))
		{
			object source = value!;
			RuntimeParamSource members
				= RuntimeParamSource.Of(source.GetType());
			for (int i = 0; i < members.Names.Length; i++)
			{
				Add(
					command,
					members.Names[i],
					members.Getters[i](source),
					members.Types[i]);
			}

			return;
		}

		string[] names = ParamSource<T>.Names;
		Func<T, object?>[] getters = ParamSource<T>.Getters;
		Type[] types = ParamSource<T>.Types;
		for (int i = 0; i < names.Length; i++)
			Add(command, names[i], getters[i](value), types[i]);
	}

	// A NULL carries no type, so it is sent with the type of its argument,
	// as the command builders do.
	static void Add(
		DbCommand command,
		string name,
		object? value,
		Type clrType)
	{
		DbParameter parameter = command.CreateParameter();
		parameter.ParameterName = name;
		if (value is null)
		{
			parameter.Value = DBNull.Value;
			if (ClrDbTypes.From(clrType) is { } nullType)
				parameter.DbType = nullType;
		}
		else
		{
			parameter.Value = value;
		}

		command.Parameters.Add(parameter);
	}

	// The first node, which AddParam or AddParams on a bound query returns.
	// It alone holds the definition and the source.
	sealed class Root<T>(
		DbQuery definition,
		object source,
		string? name,
		T value)
		: DbQueryCall
	{
		readonly DbQuery _definition = definition;
		readonly object _source = source;
		readonly string? _name = name;
		readonly T _value = value;

		internal override DbQuery Definition => _definition;

		internal override object SourceCore => _source;

		internal override void BindAll(DbCommand command)
			=> BindValue(command, _name, _value);

		public override bool Equals(object? obj)
			=> obj is Root<T> other
			&& _definition.Equals(other._definition)
			&& Equals(_source, other._source)
			&& string.Equals(_name, other._name, StringComparison.Ordinal)
			&& EqualityComparer<T>.Default.Equals(_value, other._value);

		public override int GetHashCode()
		{
			unchecked
			{
				int h = _definition.GetHashCode();
				h = (h * 31) + _source.GetHashCode();
				h = (h * 31) + (_name?.GetHashCode() ?? 0);
				return (h * 31) + EqualityComparer<T>.Default.GetHashCode(_value!);
			}
		}
	}

	// Every later node: its parent, a name and a value.
	// The value stays typed, so this layer boxes nothing.
	sealed class Node<T>(DbQueryCall parent, string? name, T value)
		: DbQueryCall
	{
		readonly DbQueryCall _parent = parent;
		readonly string? _name = name;
		readonly T _value = value;

		internal override DbQuery Definition => _parent.Definition;

		internal override object SourceCore => _parent.SourceCore;

		internal override void BindAll(DbCommand command)
		{
			_parent.BindAll(command);
			BindValue(command, _name, _value);
		}

		public override bool Equals(object? obj)
			=> obj is Node<T> other
			&& _parent.Equals(other._parent)
			&& string.Equals(_name, other._name, StringComparison.Ordinal)
			&& EqualityComparer<T>.Default.Equals(_value, other._value);

		public override int GetHashCode()
		{
			unchecked
			{
				int h = _parent.GetHashCode();
				h = (h * 31) + (_name?.GetHashCode() ?? 0);
				return (h * 31) + EqualityComparer<T>.Default.GetHashCode(_value!);
			}
		}
	}

	CancellationToken IExecuteReader.CancellationToken => default;

	bool IExecuteReaderAsync.UseAsyncRead => false;

	TResult IExecuteReader.ExecuteReader<TState, TResult>(
		TState state,
		Func<IDataReader, TState, TResult> transform,
		CommandBehavior behavior)
		=> DbQueryRunner.Run(
			Definition,
			SourceCore,
			this,
			state,
			transform,
			behavior);

	ValueTask<TResult> IExecuteReader.ExecuteReaderAsync<TState, TResult>(
		TState state,
		Func<IDataReader, TState, ValueTask<TResult>> transform,
		CommandBehavior behavior,
		CancellationToken cancellationToken)
		=> DbQueryRunner.RunAsync(
			Definition,
			SourceCore,
			this,
			state,
			transform,
			behavior,
			cancellationToken);

	ValueTask<TResult> IExecuteReader.ExecuteReaderAsync<TState, TResult>(
		TState state,
		Func<IDataReader, TState, ValueTask<TResult>> transform,
		CommandBehavior behavior)
		=> DbQueryRunner.RunAsync(
			Definition,
			SourceCore,
			this,
			state,
			transform,
			behavior,
			default);

	void IExecuteReader.ExecuteReader(
		Action<IDataReader> handler,
		CommandBehavior behavior)
		=> DbQueryRunner.Run(
			Definition,
			SourceCore,
			this,
			handler,
			behavior);

	T IExecuteReader.ExecuteReader<T>(
		Func<IDataReader, T> transform,
		CommandBehavior behavior)
		=> DbQueryRunner.Run(
			Definition,
			SourceCore,
			this,
			transform,
			behavior);

	ValueTask IExecuteReader.ExecuteReaderAsync(
		Func<IDataReader, ValueTask> handler,
		CommandBehavior behavior)
		=> DbQueryRunner.RunAsync(
			Definition,
			SourceCore,
			this,
			handler,
			behavior);

	ValueTask<T> IExecuteReader.ExecuteReaderAsync<T>(
		Func<IDataReader, ValueTask<T>> transform,
		CommandBehavior behavior)
		=> DbQueryRunner.RunAsync(
			Definition,
			SourceCore,
			this,
			transform,
			behavior);
}
