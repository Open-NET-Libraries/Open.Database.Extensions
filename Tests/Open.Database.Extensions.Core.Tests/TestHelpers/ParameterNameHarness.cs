using System.Data.Common;

namespace Open.Database.Extensions.Tests;

#nullable enable

[ExcludeFromCodeCoverage]
internal static class ParameterNameHarness
{
	static int _procedureCounter;

	// A unique procedure name per run: remembered names are shared by every command that uses the same connection source and procedure name.
	internal static string NewProcedure() => "dbo.names" + Interlocked.Increment(ref _procedureCounter);
}

/// <summary>
/// Runs commands on a fake provider that reports <c>real</c> as the parameter names of one procedure. Every <see cref="Run"/> is a new command on the
/// same connection and procedure, so the second run reads the names from memory; <see cref="Lookups"/> counts the times the provider was asked.
/// </summary>
[ExcludeFromCodeCoverage]
internal sealed class SyncNameRun
{
	readonly string _procedure = ParameterNameHarness.NewProcedure();
	readonly CommandType _type;
	readonly string[] _real;

	public SyncNameRun(CommandType type, params string[] real)
	{
		_type = type;
		_real = real;
	}

	public SyncNameRun(params string[] real) : this(CommandType.StoredProcedure, real) { }

	public ConnectionProbe Probe { get; } = new(ConnectionState.Open);

	public int Lookups { get; private set; }

	/// <summary>The parameters of the last run, by name.</summary>
	public Dictionary<string, object?> Values => Probe.Parameters.ToDictionary(p => p.ParameterName, p => p.Value);

	/// <summary>Executes a new command and returns the names of the parameters it sent, in order.</summary>
	public string[] Run(Action<ExpressiveCommand> add)
	{
		Probe.Parameters.Clear();
		var command = new DerivedParameterNameTests.ProcCommand(Probe.Connection, _type, _procedure, _real);
		add(command);
		try { command.ExecuteNonQuery(); }
		finally { Lookups += command.Lookups; }

		return [.. Probe.Parameters.Select(p => p.ParameterName)];
	}

	public string[] Add(string expression) => Run(c => c.AddParam(value: 1, expression: expression));

	public string[] AddIf(string expression, bool condition = true) => Run(c => c.AddParamIf(condition, value: 1, expression: expression));
}

/// <summary>The asynchronous twin of <see cref="SyncNameRun"/>: the same contract through <c>ExecuteNonQueryAsync</c> and the asynchronous name lookup.</summary>
[ExcludeFromCodeCoverage]
internal sealed class AsyncNameRun
{
	sealed class Command(DbConnection connection, CommandType type, string procedure, string[] names)
		: ExpressiveDbCommand(connection, type, procedure)
	{
		readonly ImmutableArray<string> _names = [.. names];

		public int SyncLookups { get; private set; }
		public int AsyncLookups { get; private set; }

		protected override ImmutableArray<string> GetProcedureParameterNames(DbConnection connection, string procedure)
		{
			SyncLookups++;
			return _names;
		}

		protected override ValueTask<ImmutableArray<string>> GetProcedureParameterNamesAsync(DbConnection connection, string procedure, CancellationToken cancellationToken)
		{
			AsyncLookups++;
			return new(_names);
		}
	}

	readonly string _procedure = ParameterNameHarness.NewProcedure();
	readonly CommandType _type;
	readonly string[] _real;
	readonly DerivedParameterNameTests.RecordingDbCommand _db = new();
	readonly DerivedParameterNameTests.OpenDbConnection _connection;

	public AsyncNameRun(CommandType type, params string[] real)
	{
		_type = type;
		_real = real;
		_connection = new(_db);
	}

	public AsyncNameRun(params string[] real) : this(CommandType.StoredProcedure, real) { }

	/// <summary>How many times the provider was asked, synchronously or asynchronously.</summary>
	public int Lookups { get; private set; }

	/// <summary>How many of those lookups were the synchronous one (none, on the asynchronous path).</summary>
	public int SyncLookups { get; private set; }

	/// <summary>The parameters of the last run, by name.</summary>
	public Dictionary<string, object?> Values => _db.Sent.ToDictionary(p => p.ParameterName, p => p.Value);

	public async Task<string[]> RunAsync(Action<ExpressiveDbCommand> add)
	{
		_db.Sent.Clear();
		var command = new Command(_connection, _type, _procedure, _real);
		add(command);
		try { await command.ExecuteNonQueryAsync(); }
		finally
		{
			Lookups += command.SyncLookups + command.AsyncLookups;
			SyncLookups += command.SyncLookups;
		}

		return [.. _db.Sent.Select(p => p.ParameterName)];
	}

	public Task<string[]> AddAsync(string expression) => RunAsync(c => c.AddParam(value: 1, expression: expression));

	public Task<string[]> AddIfAsync(string expression, bool condition = true) => RunAsync(c => c.AddParamIf(condition, value: 1, expression: expression));
}
