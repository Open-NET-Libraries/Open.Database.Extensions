using Dapper;
using Microsoft.Data.SqlClient;

namespace Open.Database.Extensions.Benchmarks;

// The OdeBench_* databases of the BenchmarkDotNet classes live in the scratch folder (never in the user profile on drive C:,
// which is where LocalDB puts the files of a plain CREATE DATABASE). Same rule as RealProviderBenchmarks: if the scratch folder is
// missing or CREATE DATABASE fails, the run stops with a clear message; there is no fallback.
static class LocalDbScratch
{
	const string Prefix = "OdeBench_";
	const string DataSize = "size = 16MB, filegrowth = 16MB", LogSize = "size = 8MB, filegrowth = 8MB";

	static string Folder()
		=> RealProviderParams.Get(RealProviderParams.ScratchVariable) is { } scratch
			? Path.GetFullPath(scratch)
			: throw new InvalidOperationException("STOP: no scratch folder for the SQL Server database files. Set ODX_RP_SCRATCH to a folder that is not on drive C:. There is no fallback to the default location.");

	/// <summary>Drops OdeBench_* databases left behind by a run that died, but only those whose files are in the scratch folder.</summary>
	public static void Sweep(string server)
	{
		string folder = Folder();
		using var master = new SqlConnection(server);
		master.Open();
		var leftovers = master.Query<string>(
			"select distinct d.name from sys.databases d join sys.master_files f on f.database_id = d.database_id where d.name like N'OdeBench[_]%' and f.physical_name like @folder + N'%'",
			new { folder }).AsList();
		foreach (string name in leftovers)
		{
			master.Execute($"alter database [{name}] set single_user with rollback immediate; drop database [{name}]");
			Console.WriteLine($"swept leftover database {name} (files were under {folder})");
		}
	}

	/// <summary>Creates a new OdeBench_* database whose data and log files are in the scratch folder, and returns its name.</summary>
	public static string Create(string server)
	{
		string folder = Folder();
		string name = Prefix + Guid.NewGuid().ToString("N");
		string data = Path.Combine(folder, name + ".mdf"), log = Path.Combine(folder, name + "_log.ldf");
		try
		{
			Directory.CreateDirectory(folder);
			using var master = new SqlConnection(server);
			master.Open();
			master.Execute($"create database [{name}] on (name = N'{name}', filename = N'{In6Probe.Quote(data)}', {DataSize}) log on (name = N'{name}_log', filename = N'{In6Probe.Quote(log)}', {LogSize})");
		}
		catch (Exception ex)
		{
			throw new InvalidOperationException($"STOP: could not CREATE DATABASE [{name}] with its files in the scratch folder {folder}: {ex.GetType().Name}: {ex.Message} There is no fallback to the default location.", ex);
		}

		return name;
	}
}
