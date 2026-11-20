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
	// How a node's parameters are named.
	// Explicit: the name the caller wrote.
	// Derived: the argument's text, resolved to the real name when it runs.
	// Members: the public members of the value, each resolved the same way.
	enum NameKind
	{
		Explicit,
		Derived,
		Members,
	}

	private protected DbQueryCall() { }

	internal abstract DbQuery Definition { get; }

	internal abstract object SourceCore { get; }

	// Whether any node needs the real names (a derived name or members).
	internal abstract bool NeedsNames { get; }

	// False when a node that needs a name matches none of the remembered names
	// (see ExpressiveCommandBase.Fits).
	internal abstract bool Fits(NameSet names, string command);

	// Whether a node of the whole call claims the name with a name of its own
	// (one written or derived, not a member of an AddParams object): such a
	// parameter wins over a member that resolves to the same name, whichever was
	// added first, as on a command.
	internal abstract bool Claims(string name, NameSet? names, DbQuery definition);

	// Binds this node's parameters after its parent's, in the order added.
	// `whole` is the call that runs: the node last added.
	internal abstract void BindAll(
		DbCommand command,
		DbQuery definition,
		NameSet? names,
		DbQueryCall whole);

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
		=> new DerivedNode<T>(this, CheckedExpression(expression), value);

	internal static DbQueryCall NewRoot<T>(
		DbQuery definition,
		object source,
		string? name,
		T value)
		=> new Root<T>(definition, source, name, value);

	internal static DbQueryCall NewDerivedRoot<T>(
		DbQuery definition,
		object source,
		string expression,
		T value)
		=> new DerivedRoot<T>(definition, source, expression, value);

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

	// The text of the argument of AddParam(value), which the compiler
	// supplies; the facts of an expression are worked out once per text.
	// The name it stands for is settled when the query runs.
	[SuppressMessage(
		"Usage",
		"CA2208:Instantiate argument exceptions correctly")]
	internal static string CheckedExpression(string? expression)
		=> ParameterNames.Describe(expression) is not null
			? expression!
			: throw (expression is null
				? new ArgumentException(
					"The compiler did not supply the argument's text (deriving a name needs C# 10 or newer). Pass the name explicitly: AddParam(\"name\", value).",
					"value")
				: new ArgumentException(
					$"Cannot derive a parameter name from the argument '{expression}'. Pass a variable or a chain of members such as filter.AuthorId, or name the parameter: AddParam(\"name\", value).",
					"value"));

	// The real name an argument of AddParam(value) stands for.
	static string ResolveDerived(
		string expression,
		NameSet? names,
		DbQuery definition)
		=> Known(names)
			? names!.Match(expression, definition.CommandText)
				?? throw NoMatch(expression, names!, definition)
			: ParameterNames.Describe(expression)!.LastMember;

	static bool ClaimsValue(
		NameKind kind,
		string? own,
		string name,
		NameSet? names,
		DbQuery definition)
		=> kind switch
		{
			NameKind.Explicit => SameName(own!, name),
			NameKind.Derived
				=> SameName(ResolveDerived(own!, names, definition), name),
			_ => false,
		};

	static bool Known(NameSet? names)
		=> names is { Names.Length: > 0 };

	static bool FitsValue<T>(
		NameKind kind,
		string? name,
		T value,
		NameSet names,
		string command)
	{
		if (names.Names.Length == 0) return true;

		switch (kind)
		{
			case NameKind.Derived:
				return names.Match(name!, command) is not null;
			case NameKind.Members:
				// A second look drops a member the command does not take
				// without reading again.
				if (names.Rechecked) return true;
				foreach (string member in MemberNames(value))
				{
					if (names.Match(member, command) is null) return false;
				}

				return true;
			default:
				return true;
		}
	}

	static string[] MemberNames<T>(T value)
		=> typeof(T) == typeof(object)
			? RuntimeParamSource.Of(value!.GetType()).Names
			: ParamSource<T>.Names;

	static void BindValue<T>(
		DbCommand command,
		NameKind kind,
		string? name,
		T value,
		DbQuery definition,
		NameSet? names,
		DbQueryCall whole)
	{
		switch (kind)
		{
			case NameKind.Explicit:
				Add(command, name!, value, typeof(T), definition, names);
				return;
			case NameKind.Derived:
				Add(
					command,
					ResolveDerived(name!, names, definition),
					value,
					typeof(T),
					definition,
					names);
				return;
			default:
				BindMembers(command, value, definition, names, whole);
				return;
		}
	}

	// The members of the values, found once per type. When the real names are
	// known a member the command does not take is not sent, and a member
	// whose name another parameter claims (one written or derived, added before
	// or after) is left to that parameter. Two members of one name collide.
	static void BindMembers<T>(
		DbCommand command,
		T value,
		DbQuery definition,
		NameSet? names,
		DbQueryCall whole)
	{
		bool known = Known(names);
		if (typeof(T) == typeof(object))
		{
			object source = value!;
			RuntimeParamSource members
				= RuntimeParamSource.Of(source.GetType());
			for (int i = 0; i < members.Names.Length; i++)
			{
				string? name = known
					? names!.Match(members.Names[i], definition.CommandText)
					: members.Names[i];
				if (name is null || whole.Claims(name, names, definition)) continue;

				Add(
					command,
					name,
					members.Getters[i](source),
					members.Types[i],
					definition,
					names);
			}

			return;
		}

		string[] memberNames = ParamSource<T>.Names;
		Func<T, object?>[] getters = ParamSource<T>.Getters;
		Type[] types = ParamSource<T>.Types;
		string?[]? resolved = known
			? Resolved<T>(definition, names!, memberNames)
			: null;
		for (int i = 0; i < memberNames.Length; i++)
		{
			string? name = known ? resolved![i] : memberNames[i];
			if (name is null || whole.Claims(name, names, definition)) continue;

			Add(command, name, getters[i](value), types[i], definition, names);
		}
	}

	// The real name of each member in member order, matched once per
	// definition, values type and set of names.
	static string?[] Resolved<T>(
		DbQuery definition,
		NameSet names,
		string[] members)
	{
		DbQuery.MemberMatch[]? known = definition.Matches;
		int kept = known?.Length ?? 0;
		for (int i = 0; i < kept; i++)
		{
			DbQuery.MemberMatch m = known![i];
			if (ReferenceEquals(m.Names, names) && m.Type == typeof(T))
				return m.Resolved;
		}

		var resolved = new string?[members.Length];
		for (int i = 0; i < members.Length; i++)
			resolved[i] = names.Match(members[i], definition.CommandText);

		// Newest first; the oldest of four goes.
		kept = Math.Min(kept, DbQuery.MaxMatches - 1);
		var next = new DbQuery.MemberMatch[kept + 1];
		next[0] = new(typeof(T), names, resolved);
		if (kept > 0) Array.Copy(known!, 0, next, 1, kept);
		definition.Matches = next;
		return resolved;
	}

	static void Add(
		DbCommand command,
		string name,
		object? value,
		Type clrType,
		DbQuery definition,
		NameSet? names)
	{
		if (Has(command, name)) throw Collision(name, definition, names);

		Append(command, name, value, clrType);
	}

	// A NULL carries no type, so it is sent with the type of its argument,
	// as the command builders do.
	static void Append(
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

	// Two parameters of one name would overwrite each other.
	// The names are compared without an @ and ignoring case, as the
	// command builders compare them.
	static bool Has(DbCommand command, string name)
	{
		DbParameterCollection parameters = command.Parameters;
		for (int i = 0; i < parameters.Count; i++)
		{
			if (SameName(parameters[i].ParameterName, name)) return true;
		}

		return false;
	}

	static bool SameName(string a, string b)
		=> Bare(a).Equals(Bare(b), StringComparison.OrdinalIgnoreCase);

	static ReadOnlySpan<char> Bare(string name)
		=> name.Length > 0 && name[0] == '@'
			? name.AsSpan(1)
			: name.AsSpan();

	static InvalidOperationException Collision(
		string name,
		DbQuery definition,
		NameSet? names)
		=> new($"Two parameters resolve to '{name}'. "
			+ (Known(names)
				? $"'{definition.CommandText}' declares: {string.Join(", ", names!.Names)}."
				: "The parameter names are not known, so each argument is used under its last member as written.")
			+ " Name one of them explicitly: AddParam(\"name\", value).");

	static InvalidOperationException NoMatch(
		string expression,
		NameSet names,
		DbQuery definition)
		=> new($"The argument '{expression}' matches none of the parameters of '{definition.CommandText}' (tried {string.Join(", ", ParameterNames.Candidates(expression))}). It declares: {string.Join(", ", names.Names)}. Name the parameter explicitly: AddParam(\"name\", value).");

	// The first node, which AddParam or AddParams on a bound query returns.
	// It alone holds the definition and the source.
	// For a derived name `name` holds the argument's text (see DerivedRoot).
	class Root<T>(
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

		internal virtual NameKind Kind
			=> _name is null ? NameKind.Members : NameKind.Explicit;

		internal override DbQuery Definition => _definition;

		internal override object SourceCore => _source;

		internal override bool NeedsNames => Kind != NameKind.Explicit;

		internal override bool Fits(NameSet names, string command)
			=> FitsValue(Kind, _name, _value, names, command);

		internal override bool Claims(
			string name,
			NameSet? names,
			DbQuery definition)
			=> ClaimsValue(Kind, _name, name, names, definition);

		internal override void BindAll(
			DbCommand command,
			DbQuery definition,
			NameSet? names,
			DbQueryCall whole)
			=> BindValue(command, Kind, _name, _value, definition, names, whole);

		public override bool Equals(object? obj)
			=> obj is Root<T> other
			&& Kind == other.Kind
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

	// The same fields as a root: which kind it is comes from the type, so a
	// node is no larger for it.
	sealed class DerivedRoot<T>(
		DbQuery definition,
		object source,
		string expression,
		T value)
		: Root<T>(definition, source, expression, value)
	{
		internal override NameKind Kind => NameKind.Derived;
	}

	// Every later node: its parent, a name and a value.
	// The value stays typed, so this layer boxes nothing.
	class Node<T>(DbQueryCall parent, string? name, T value)
		: DbQueryCall
	{
		readonly DbQueryCall _parent = parent;
		readonly string? _name = name;
		readonly T _value = value;

		internal virtual NameKind Kind
			=> _name is null ? NameKind.Members : NameKind.Explicit;

		internal override DbQuery Definition => _parent.Definition;

		internal override object SourceCore => _parent.SourceCore;

		internal override bool NeedsNames
			=> Kind != NameKind.Explicit || _parent.NeedsNames;

		internal override bool Fits(NameSet names, string command)
			=> _parent.Fits(names, command)
			&& FitsValue(Kind, _name, _value, names, command);

		internal override bool Claims(
			string name,
			NameSet? names,
			DbQuery definition)
			=> _parent.Claims(name, names, definition)
			|| ClaimsValue(Kind, _name, name, names, definition);

		internal override void BindAll(
			DbCommand command,
			DbQuery definition,
			NameSet? names,
			DbQueryCall whole)
		{
			_parent.BindAll(command, definition, names, whole);
			BindValue(command, Kind, _name, _value, definition, names, whole);
		}

		public override bool Equals(object? obj)
			=> obj is Node<T> other
			&& Kind == other.Kind
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

	sealed class DerivedNode<T>(DbQueryCall parent, string expression, T value)
		: Node<T>(parent, expression, value)
	{
		internal override NameKind Kind => NameKind.Derived;
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
