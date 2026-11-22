namespace Open.Database.Extensions;

/// <summary>
/// Adds the parameters of a call to a <see cref="DbCommand"/>: matches the
/// names, refuses a name used twice, and creates the provider's parameters.
/// </summary>
static class DbQueryBinding
{
	/// <summary>
	/// How many (values type, set of names) matches a definition keeps.
	/// </summary>
	const int MaxMatches = 4;

	/// <summary>
	/// Determines whether the real names of the command are known.
	/// </summary>
	/// <param name="names">The remembered names, or <see langword="null"/>.</param>
	/// <returns><see langword="true"/> if there is at least one name; otherwise, <see langword="false"/>.</returns>
	public static bool Known(NameSet? names)
		=> names is { Names.Length: > 0 };

	/// <summary>
	/// Gets the real name an argument of <c>AddParam(value)</c> stands for.
	/// </summary>
	/// <param name="derived">The facts of the argument's text.</param>
	/// <param name="names">The remembered names, or <see langword="null"/> when they are not known.</param>
	/// <param name="definition">The query definition.</param>
	/// <returns>The real name; the last member as written when the names are not known.</returns>
	/// <exception cref="InvalidOperationException">The expression matches none of the names, or more than one equally well.</exception>
	public static string ResolveDerived(
		ParameterNames.DerivedExpression derived,
		NameSet? names,
		DbQuery definition)
		=> ResolveDerived(derived, names, definition, out _);

	/// <summary>
	/// Gets the real name an argument of <c>AddParam(value)</c> stands for, and its place in the names.
	/// </summary>
	/// <param name="derived">The facts of the argument's text.</param>
	/// <param name="names">The remembered names, or <see langword="null"/> when they are not known.</param>
	/// <param name="definition">The query definition.</param>
	/// <param name="entry">The place of the real name in <paramref name="names"/>, or <see langword="null"/> when the names are not known.</param>
	/// <returns>The real name; the last member as written when the names are not known.</returns>
	/// <exception cref="InvalidOperationException">The expression matches none of the names, or more than one equally well.</exception>
	public static string ResolveDerived(
		ParameterNames.DerivedExpression derived,
		NameSet? names,
		DbQuery definition,
		out NameSet.Entry? entry)
	{
		if (!Known(names))
		{
			entry = null;
			return derived.LastMember;
		}

		// The expression remembers its last answer, so a repeat in the same names reads one field.
		ParameterNames.DerivedExpression.Resolution? last = derived.Last;
		if (last is null || !ReferenceEquals(last.Names, names))
		{
			string real = names!.Match(derived.Expression, definition.CommandText)
				?? throw NoMatch(derived.Expression, names!, definition);
			last = new(names!, real, names!.Find(real));
			derived.Last = last;
		}

		entry = last.Entry;
		return last.Real;
	}

	/// <summary>
	/// Gets the names of the members of an object.
	/// </summary>
	/// <typeparam name="T">The type of the object.</typeparam>
	/// <param name="value">The object.</param>
	/// <returns>The names of the members.</returns>
	public static string[] MemberNames<T>(T value)
		=> typeof(T) == typeof(object)
			? RuntimeParamSource.Of(value!.GetType()).Names
			: ParamSource<T>.Names;

	/// <summary>
	/// Adds a parameter of a written or derived name.
	/// </summary>
	/// <param name="command">The command to add the parameter to.</param>
	/// <param name="name">The real name of the parameter.</param>
	/// <param name="entry">The name's place in <paramref name="index"/> when it is known, or <see langword="null"/>.</param>
	/// <param name="value">The value as a call holds it: boxed, or a <see cref="NullValue"/>.</param>
	/// <param name="definition">The query definition.</param>
	/// <param name="names">The remembered names, or <see langword="null"/> when they are not known.</param>
	/// <param name="index">The names the command is looked up in, or <see langword="null"/>.</param>
	/// <param name="seen">Which names of <paramref name="index"/> the command already has.</param>
	public static void AddValue(
		DbCommand command,
		string name,
		NameSet.Entry? entry,
		object value,
		DbQuery definition,
		NameSet? names,
		NameSet? index,
		ref ulong seen)
	{
		if (value is NullValue none)
			Add(command, name, entry, null, none.ClrType, definition, names, index, ref seen);
		else
			Add(command, name, entry, value, value.GetType(), definition, names, index, ref seen);
	}

	/// <summary>
	/// Adds the members of the values as parameters.
	/// </summary>
	/// <remarks>
	/// The members of the values are found once per type.
	/// When the real names are known a member the command does not take is
	/// not sent, and a member whose name another parameter claims (one written
	/// or derived, added before or after) is left to that parameter.
	/// Two members of one name collide.
	/// </remarks>
	/// <typeparam name="T">The type of the values.</typeparam>
	/// <param name="command">The command to add the parameters to.</param>
	/// <param name="value">The values.</param>
	/// <param name="definition">The query definition.</param>
	/// <param name="names">The remembered names, or <see langword="null"/> when they are not known.</param>
	/// <param name="index">The names the command is looked up in, or <see langword="null"/>.</param>
	/// <param name="whole">The entries of the whole call.</param>
	/// <param name="seen">Which names of <paramref name="index"/> the command already has.</param>
	public static void BindMembers<T>(
		DbCommand command,
		T value,
		DbQuery definition,
		NameSet? names,
		NameSet? index,
		ParamList whole,
		ref ulong seen)
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
					null,
					members.Getters[i](source),
					members.Types[i],
					definition,
					names,
					index,
					ref seen);
			}

			return;
		}

		string[] memberNames = ParamSource<T>.Names;
		Func<T, object?>[] getters = ParamSource<T>.Getters;
		Type[] types = ParamSource<T>.Types;
		DbQuery.MemberMatch? match = known
			? Resolved<T>(definition, names!, memberNames)
			: null;
		string?[]? resolved = match?.Resolved;
		for (int i = 0; i < memberNames.Length; i++)
		{
			string? name = known ? resolved![i] : memberNames[i];
			if (name is null || whole.Claims(name, names, definition)) continue;

			Add(command, name, match?.Entries[i], getters[i](value), types[i], definition, names, index, ref seen);
		}
	}

	/// <summary>
	/// Gets the real name and place of each member in member order, matched once per
	/// definition, values type and set of names.
	/// </summary>
	/// <typeparam name="T">The type of the values.</typeparam>
	/// <param name="definition">The query definition, which keeps the matches.</param>
	/// <param name="names">The remembered names.</param>
	/// <param name="members">The names of the members.</param>
	/// <returns>The match: the real name of each member (<see langword="null"/> for one the command does not take) and its place in the names.</returns>
	static DbQuery.MemberMatch Resolved<T>(
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
				return m;
		}

		var resolved = new string?[members.Length];
		var entries = new NameSet.Entry?[members.Length];
		for (int i = 0; i < members.Length; i++)
		{
			resolved[i] = names.Match(members[i], definition.CommandText);
			if (resolved[i] is { } real) entries[i] = names.Find(real);
		}

		// Newest first; the oldest of four goes.
		kept = Math.Min(kept, MaxMatches - 1);
		var next = new DbQuery.MemberMatch[kept + 1];
		next[0] = new(typeof(T), names, resolved, entries);
		if (kept > 0) Array.Copy(known!, 0, next, 1, kept);
		definition.Matches = next;
		return next[0];
	}

	static void Add(
		DbCommand command,
		string name,
		NameSet.Entry? entry,
		object? value,
		Type clrType,
		DbQuery definition,
		NameSet? names,
		NameSet? index,
		ref ulong seen)
	{
		// A name of the set is told apart by its place, in one step; any other name is looked for among the parameters the command has.
		entry ??= index?.Find(name);
		if (entry is { Index: < 64 })
		{
			ulong bit = 1UL << entry.Index;
			if ((seen & bit) != 0) throw Collision(name, definition, names);

			seen |= bit;
		}
		else if (Has(command, name))
		{
			throw Collision(name, definition, names);
		}

		Append(command, name, value, clrType);
	}

	/// <summary>
	/// Adds a parameter to the command.
	/// </summary>
	/// <remarks>
	/// A NULL carries no type, so it is sent with the type of its argument,
	/// as the command builders do.
	/// </remarks>
	/// <param name="command">The command to add the parameter to.</param>
	/// <param name="name">The name of the parameter.</param>
	/// <param name="value">The value of the parameter.</param>
	/// <param name="clrType">The CLR type of the argument.</param>
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

	/// <summary>
	/// Determines whether the command already has a parameter of the name,
	/// since two parameters of one name would overwrite each other.
	/// </summary>
	/// <param name="command">The command.</param>
	/// <param name="name">The name.</param>
	/// <returns>
	/// <see langword="true"/> if the command has the parameter; otherwise,
	/// <see langword="false"/>.
	/// </returns>
	static bool Has(DbCommand command, string name)
	{
		DbParameterCollection parameters = command.Parameters;
		for (int i = 0; i < parameters.Count; i++)
		{
			if (SameName(parameters[i].ParameterName, name)) return true;
		}

		return false;
	}

	/// <summary>
	/// Determines whether two parameter names are the same name, ignoring an
	/// <c>@</c> prefix and case.
	/// </summary>
	/// <param name="a">The first name.</param>
	/// <param name="b">The second name.</param>
	/// <returns><see langword="true"/> if they are the same name; otherwise, <see langword="false"/>.</returns>
	public static bool SameName(string a, string b)
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
}
