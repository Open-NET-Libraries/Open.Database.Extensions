using System.Globalization;
using System.Text;

namespace Open.Database.Extensions;

/// <summary>
/// Reads the parameter names of a stored procedure through the ADO.NET schema collection <c>ProcedureParameters</c>, which SQL Server (and any other provider that implements the collection) answers.
/// </summary>
/// <remarks>
/// "Not known" is an empty or default array: a procedure name with a database part (the lookup would run in the current database), a provider without the collection,
/// a procedure the provider does not list, and a name that exists in more than one schema when none was given. A failure of the lookup itself is thrown as it is, except that a `NotSupportedException`, a `NotImplementedException` or an `ArgumentException` (and an `InvalidOperationException` that names the collection) is taken as "the provider does not support the collection".
/// </remarks>
static class ProcedureSchemaNames
{
	const string Collection = "ProcedureParameters";

	internal static ImmutableArray<string> Get(DbConnection connection, string procedure)
	{
		if (Restrictions(procedure) is not { } restrictions) return default;

		try
		{
			return Names(connection.GetSchema(Collection, restrictions), schemaGiven: restrictions[1] is not null);
		}
		catch (Exception ex) when (IsUnsupported(ex))
		{
			return default;
		}
		catch (Exception ex) when (ex is not OperationCanceledException)
		{
			ex.Data["Procedure"] = procedure;
			throw;
		}
	}

#if !NET10_0_OR_GREATER
	// DbConnection.GetSchemaAsync does not exist on the netstandard targets.
	internal static ValueTask<ImmutableArray<string>> GetAsync(DbConnection connection, string procedure, CancellationToken cancellationToken)
	{
		cancellationToken.ThrowIfCancellationRequested();
		return new(Get(connection, procedure));
	}
#else
	internal static async ValueTask<ImmutableArray<string>> GetAsync(DbConnection connection, string procedure, CancellationToken cancellationToken)
	{
		if (Restrictions(procedure) is not { } restrictions) return default;

		try
		{
			DataTable table = await connection.GetSchemaAsync(Collection, restrictions, cancellationToken).ConfigureAwait(false);
			return Names(table, schemaGiven: restrictions[1] is not null);
		}
		catch (Exception ex) when (IsUnsupported(ex))
		{
			return default;
		}
		catch (Exception ex) when (ex is not OperationCanceledException && !cancellationToken.IsCancellationRequested)
		{
			ex.Data["Procedure"] = procedure;
			throw;
		}
	}
#endif

	// A provider that has no such collection says so in one of these ways; an InvalidOperationException only when it names the collection (a closed connection is not "unsupported").
	static bool IsUnsupported(Exception ex)
		=> ex is NotSupportedException or NotImplementedException or ArgumentException
			|| (ex is InvalidOperationException && MentionsCollection(ex.Message));

	static bool MentionsCollection(string message)
#if NETSTANDARD2_0
		=> message.IndexOf(Collection, StringComparison.OrdinalIgnoreCase) >= 0;
#else
		=> message.Contains(Collection, StringComparison.OrdinalIgnoreCase);
#endif

	// The restrictions are catalog, schema, name and parameter. Null: the name is not looked up (a database part, or an empty or unclosed part).
	static string?[]? Restrictions(string procedure)
	{
		List<string>? parts = Split(procedure);
		if (parts is null || parts.Count > 2 || parts.Exists(static p => p.Length == 0)) return null;

		return parts.Count == 1
			? [null, null, parts[0], null]
			: [null, parts[0], parts[1], null];
	}

	// Splits on the dots outside [brackets] and "quotes", which are removed (a doubled closing character inside is one character of the name).
	static List<string>? Split(string name)
	{
		List<string> parts = [];
		var part = new StringBuilder();
		for (int i = 0; i < name.Length; i++)
		{
			char c = name[i];
			if (c is '[' or '"')
			{
				char close = c == '[' ? ']' : '"';
				for (i++; ; i++)
				{
					if (i >= name.Length) return null;
					if (name[i] != close)
					{
						part.Append(name[i]);
					}
					else if (i + 1 < name.Length && name[i + 1] == close)
					{
						part.Append(close);
						i++;
					}
					else
					{
						break;
					}
				}
			}
			else if (c == '.')
			{
				parts.Add(part.ToString());
				part.Clear();
			}
			else
			{
				part.Append(c);
			}
		}

		parts.Add(part.ToString());
		return parts;
	}

	// The rows with a name and an ordinal above 0 (ordinal 0 is the return value), in ordinal order. When no schema was given, rows from more than one schema are ambiguous.
	static ImmutableArray<string> Names(DataTable? table, bool schemaGiven)
	{
		if (table is null) return default;

		DataColumnCollection columns = table.Columns;
		if (!columns.Contains("PARAMETER_NAME") || !columns.Contains("ORDINAL_POSITION")) return default;

		bool checkSchema = !schemaGiven && columns.Contains("SPECIFIC_SCHEMA");
		string? schema = null;
		List<KeyValuePair<int, string>> found = [];
		foreach (DataRow row in table.Rows)
		{
			if (checkSchema)
			{
				string current = row["SPECIFIC_SCHEMA"] as string ?? "";
				schema ??= current;
				if (!string.Equals(schema, current, StringComparison.OrdinalIgnoreCase)) return default;
			}

			if (row["PARAMETER_NAME"] is not string { Length: > 0 } name || row["ORDINAL_POSITION"] is DBNull) continue;

			int ordinal = Convert.ToInt32(row["ORDINAL_POSITION"], CultureInfo.InvariantCulture);
			if (ordinal > 0) found.Add(new(ordinal, name));
		}

		if (found.Count == 0) return default;

		found.Sort(static (a, b) => a.Key.CompareTo(b.Key));
		ImmutableArray<string>.Builder names = ImmutableArray.CreateBuilder<string>(found.Count);
		foreach (KeyValuePair<int, string> pair in found) names.Add(pair.Value);
		return names.MoveToImmutable();
	}
}
