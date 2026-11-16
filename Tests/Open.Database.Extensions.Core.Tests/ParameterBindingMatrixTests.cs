extern alias mssql;

using Microsoft.CodeAnalysis;
using Microsoft.CodeAnalysis.CSharp;
using Microsoft.CodeAnalysis.CSharp.Syntax;

namespace Open.Database.Extensions.Tests;

#nullable enable

// What the AddParam overload set binds to. Snippets are compiled with
// Roslyn at each language version, and the symbol each call binds to is
// compared. The LegacyCompiler project does the same with the real
// compilers of the older targets (C# 7.3, 9, 12, 13) and checks the
// values and database types.
//
// A5 keeps `direction`, the no-value forms and the untyped int and long
// forms. It removes only the dead parts (the Param constructor, the
// IEnumerable<Param> constructors, the untyped ExecuteReturn) and makes
// the typed-NULL form take `object? value`. This pins that:
// - an int or long value, with and without a database type, binds to
//   the int and long forms: ("x", i), ("x", i, null),
//   ("x", i, nullableType), ("x", 0), ("x", 0L);
// - a user enum binds to a value form;
// - a typed NULL byte array compiles warning-free;
// - the removed members no longer compile.
[ExcludeFromCodeCoverage]
public static class ParameterBindingMatrixTests
{
	public static TheoryData<LanguageVersion> Versions
		=> [
			LanguageVersion.CSharp7_3,
			LanguageVersion.CSharp9,
			LanguageVersion.CSharp12,
			LanguageVersion.CSharp13,
			LanguageVersion.CSharp14,
		];

	static readonly Lazy<MetadataReference[]> References = new(static () =>
	{
		var paths = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
		if (AppContext.GetData("TRUSTED_PLATFORM_ASSEMBLIES") is string tpa)
		{
			foreach (string path in tpa.Split(Path.PathSeparator))
				paths.Add(path);
		}

		paths.Add(typeof(ExpressiveCommand).Assembly.Location);
		return [.. paths.Select(static p => (MetadataReference)MetadataReference.CreateFromFile(p))];
	});

	// The same, plus the Microsoft.Data.SqlClient package (the other
	// package defines a type of the same name, so it is not referenced).
	static readonly Lazy<MetadataReference[]> SqlReferences = new(static () =>
		[.. References.Value, MetadataReference.CreateFromFile(typeof(mssql::Open.Database.Extensions.ExpressiveSqlCommand).Assembly.Location)]);

	// Each statement is compiled in a method with: c (ExpressiveCommand),
	// q (the SQL Server command, when sql is true), s (string), i (int),
	// l (long), t (DbType?), st (SqlDbType?).
	static (CSharpCompilation Compilation, SyntaxTree Tree) Compile(LanguageVersion version, string statements, bool sql = false)
	{
		// C# 7.3 has no nullable annotations: the snippet is then written
		// without them and without the nullable context.
		bool nullable = version >= LanguageVersion.CSharp8;
		if (!nullable)
			statements = statements.Replace("[]?)", "[])").Replace(">?)", ">)").Replace("null!", "null");

		string source = $$"""
			{{(nullable ? "#nullable enable" : "")}}
			using System;
			using System.Collections.Generic;
			using System.Data;
			using Open.Database.Extensions;
			enum Mode { A }
			static class Probe
			{
				static void M(ExpressiveCommand c, {{(sql ? "Open.Database.Extensions.ExpressiveSqlCommand" : "ExpressiveCommand")}} q, string s, int i, long l, DbType? t, SqlDbType? st)
				{
					{{statements}}
				}
			}
			""";
		var tree = CSharpSyntaxTree.ParseText(source, new CSharpParseOptions(version));
		var compilation = CSharpCompilation.Create(
			"MatrixProbe",
			[tree],
			sql ? SqlReferences.Value : References.Value,
			new CSharpCompilationOptions(OutputKind.DynamicallyLinkedLibrary, nullableContextOptions: nullable ? NullableContextOptions.Enable : NullableContextOptions.Disable));
		return (compilation, tree);
	}

	// Reference-type annotations only exist from C# 8: compare without them.
	static string Normalize(string symbol) => symbol.Replace("object?", "object");

	// The symbol the first call in the statement binds to, written as its
	// name, type arguments and parameter types; "ERROR CSnnnn" for the
	// first error.
	static string Bound(LanguageVersion version, string statement, bool sql = false)
	{
		var (compilation, tree) = Compile(version, statement, sql);
		var errors = compilation.GetDiagnostics(TestContext.Current.CancellationToken).Where(static d => d.Severity == DiagnosticSeverity.Error).ToArray();
		if (errors.Length != 0) return "ERROR " + errors[0].Id;

		var call = tree.GetRoot().DescendantNodes().OfType<InvocationExpressionSyntax>().First();
		var method = (IMethodSymbol)compilation.GetSemanticModel(tree).GetSymbolInfo(call, TestContext.Current.CancellationToken).Symbol!;
		string shape = string.Join(", ", method.Parameters.Select(static p => p.Type.ToDisplayString(SymbolDisplayFormat.MinimallyQualifiedFormat)));
		string args = method.TypeArguments.Length == 0 ? "" : "<" + string.Join(", ", method.TypeArguments.Select(static t => t.ToDisplayString(SymbolDisplayFormat.MinimallyQualifiedFormat))) + ">";
		return Normalize($"{method.Name}{args}({shape})");
	}

	// Fails with the actual symbol in the message.
	static void Expect(LanguageVersion version, string expected, string statement, bool sql = false)
	{
		string actual = Bound(version, statement, sql);
		Assert.True(expected == actual, $"{version}: {statement} bound {actual}, expected {expected}");
	}

	// ---- an int or long value binds to the int and long forms ----

	[Theory]
	[MemberData(nameof(Versions))]
	public static void AnIntOrLongValue_BindsToTheIntAndLongForms_AtEveryLanguageVersion(LanguageVersion version)
	{
		Expect(version, "AddParam(string, int, DbType?, ParameterDirection)", "c.AddParam(\"x\", i);");
		Expect(version, "AddParam(string, int, DbType?, ParameterDirection)", "c.AddParam(\"x\", i, null);");
		Expect(version, "AddParam(string, int, DbType?, ParameterDirection)", "c.AddParam(\"x\", i, t);");
		Expect(version, "AddParam(string, int, DbType?, ParameterDirection)", "c.AddParam(\"x\", 0);");
		Expect(version, "AddParam(string, long, DbType?, ParameterDirection)", "c.AddParam(\"x\", 0L);");
		Expect(version, "AddParam(string, long, DbType?, ParameterDirection)", "c.AddParam(\"x\", l);");
		Expect(version, "AddParam(string, int, DbType, ParameterDirection)", "c.AddParam(\"x\", i, DbType.Int32);");
		Expect(version, "AddParamIf(bool, string, int, DbType?, ParameterDirection)", "c.AddParamIf(true, \"x\", i, null);");
		Expect(version, "AddParamIf(bool, string, int, DbType?, ParameterDirection)", "c.AddParamIf(true, \"x\", 0);");
		Expect(version, "AddParamIf(bool, string, long, DbType?, ParameterDirection)", "c.AddParamIf(true, \"x\", 0L);");
	}

	[Theory]
	[MemberData(nameof(Versions))]
	public static void AnIntOrLongValue_OnTheSqlServerCommand_BindsToTheIntAndLongForms(LanguageVersion version)
	{
		Expect(version, "AddParam(string, int, SqlDbType?, ParameterDirection)", "q.AddParam(\"x\", i);", sql: true);
		Expect(version, "AddParam(string, int, SqlDbType?, ParameterDirection)", "q.AddParam(\"x\", i, null);", sql: true);
		Expect(version, "AddParam(string, int, SqlDbType?, ParameterDirection)", "q.AddParam(\"x\", i, st);", sql: true);
		Expect(version, "AddParam(string, int, SqlDbType?, ParameterDirection)", "q.AddParam(\"x\", 0);", sql: true);
		Expect(version, "AddParam(string, long, SqlDbType?, ParameterDirection)", "q.AddParam(\"x\", 0L);", sql: true);
	}

	// ---- a user enum is a value ----

	[Theory]
	[MemberData(nameof(Versions))]
	public static void AUserEnum_BindsToAValueForm(LanguageVersion version)
	{
		string[] valueForms =
		[
			"AddParam<Mode>(string, Mode?, DbType?, ParameterDirection)",
			"AddParam(string, object, ParameterDirection)",
		];
		Assert.Contains(Bound(version, "c.AddParam(\"x\", Mode.A);"), valueForms);
	}

	// ---- the removed members no longer compile ----

	[Theory]
	[MemberData(nameof(Versions))]
	public static void TheRemovedMembers_NoLongerCompile(LanguageVersion version)
	{
		string[] removed =
		[
			// the untyped ExecuteReturn
			"c.ExecuteReturn();",
			"c.ExecuteReturnAsync();",
			// the Param constructor and the IEnumerable<Param> constructors
			"var p = new ExpressiveCommandBase<IDbConnection, IDbCommand, IDataReader, DbType, ExpressiveCommand>.Param(\"x\", 1);",
			"var d = new ExpressiveCommand((IDbConnection)null!, CommandType.Text, \"x\", (IEnumerable<ExpressiveCommandBase<IDbConnection, IDbCommand, IDataReader, DbType, ExpressiveCommand>.Param>?)null);",
		];

		foreach (string statement in removed)
			Assert.True(Bound(version, statement).StartsWith("ERROR", StringComparison.Ordinal), $"{version}: {statement}");
	}

	// ---- a typed NULL (DS2 #8): object? value ----

	[Theory]
	[MemberData(nameof(Versions))]
	public static void ATypedNullByteArray_CompilesWithNoWarning_AndBindsToTheObjectForm(LanguageVersion version)
	{
		const string statement = "c.AddParam(\"b\", (byte[]?)null, DbType.Binary);";
		var (compilation, _) = Compile(version, statement);
		var problems = compilation.GetDiagnostics(TestContext.Current.CancellationToken).Where(static d => d.Severity >= DiagnosticSeverity.Warning).Select(static d => d.ToString()).ToArray();
		Assert.True(problems.Length == 0, $"{version}: {string.Join(" | ", problems)}");
		Expect(version, "AddParam(string, object, DbType, ParameterDirection)", statement);
	}

	[Fact]
	public static void ATypedNullByteArray_IsSentAsDBNull_WithItsType()
	{
		var cmd = new ExpressiveCommand(Substitute.For<IDbConnection>(), CommandType.Text, "x");
		cmd.AddParam("b", (byte[]?)null, DbType.Binary);

		var p = Assert.Single(cmd.Params);
		Assert.Equal(DBNull.Value, p.Value);
		Assert.Equal(DbType.Binary, p.Type);
		Assert.Equal(ParameterDirection.Input, p.Direction);
	}
}
