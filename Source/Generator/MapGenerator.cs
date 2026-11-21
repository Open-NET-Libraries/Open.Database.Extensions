using System.Collections.Immutable;
using System.Threading;
using Microsoft.CodeAnalysis;
using Microsoft.CodeAnalysis.CSharp;
using Microsoft.CodeAnalysis.CSharp.Syntax;
using Microsoft.CodeAnalysis.Diagnostics;

namespace Open.Database.Extensions.Generator;

/// <summary>
/// PROTOTYPE (SG phase A). Intercepts each plain <c>x.Map&lt;T&gt;()</c> call bound to <c>Open.Database.Extensions.CoreExtensions</c>
/// (receivers <c>IExecuteReader</c>, <c>IDbCommand</c>, <c>IDataReader</c>) and serves it with a generated row reader through the
/// existing <c>Map(Func&lt;IDataRecord, T&gt;)</c> overload. Emits nothing (the call keeps running today's code) when:
/// C# is below 12, the interceptors feature is not enabled for <c>Open.Database.Extensions.Generated</c>, <c>OdxGenerateMaps</c> is false,
/// or T is a shape the generated code does not describe exactly.
/// </summary>
[Generator(LanguageNames.CSharp)]
public sealed class MapGenerator : IIncrementalGenerator
{
	/// <summary>The namespace of the generated interceptors; the consuming project must list it in <c>InterceptorsNamespaces</c>.</summary>
	public const string GeneratedNamespace = "Open.Database.Extensions.Generated";

	static readonly SymbolDisplayFormat Fq = SymbolDisplayFormat.FullyQualifiedFormat;

	/// <inheritdoc/>
	public void Initialize(IncrementalGeneratorInitializationContext context)
	{
		IncrementalValueProvider<Options> options = context.ParseOptionsProvider
			.Combine(context.AnalyzerConfigOptionsProvider)
			.Select(static (pair, _) => ReadOptions(pair.Left, pair.Right));

		IncrementalValuesProvider<Site> sites = context.SyntaxProvider
			.CreateSyntaxProvider(static (node, _) => IsCandidate(node), static (ctx, ct) => Transform(ctx, ct))
			.Where(static s => s is not null)
			.Select(static (s, _) => s!);

		context.RegisterSourceOutput(sites.Collect().Combine(options), static (spc, pair) =>
		{
			(ImmutableArray<Site> all, Options o) = pair;
			if (!o.Enabled || all.IsEmpty) return;
			spc.AddSource("OdxMaps.g.cs", Emitter.Emit(all, o));
		});
	}

	static Options ReadOptions(ParseOptions parse, AnalyzerConfigOptionsProvider config)
	{
		bool language = parse is CSharpParseOptions { LanguageVersion: >= LanguageVersion.CSharp12 };

		// Without the namespace in InterceptorsNamespaces the compiler would fail the build (CS9137): emit nothing instead.
		bool allowed = false;
		foreach (string key in new[] { "InterceptorsNamespaces", "InterceptorsPreviewNamespaces" })
		{
			if (!parse.Features.TryGetValue(key, out string? value)) continue;
			foreach (string part in value.Split(';'))
				if (part.Trim() == GeneratedNamespace) allowed = true;
		}

		config.GlobalOptions.TryGetValue("build_property.OdxGenerateMaps", out string? generate);
		config.GlobalOptions.TryGetValue("build_property.OdxGeneratorCounters", out string? counters);
		bool off = string.Equals(generate?.Trim(), "false", StringComparison.OrdinalIgnoreCase);
		return new Options(language && allowed && !off, string.Equals(counters?.Trim(), "true", StringComparison.OrdinalIgnoreCase));
	}

	// x.Map<T>(): a generic name with one type argument and no arguments. The static form CoreExtensions.Map<T>(x) is not intercepted.
	static bool IsCandidate(SyntaxNode node)
		=> node is InvocationExpressionSyntax
		{
			Expression: MemberAccessExpressionSyntax { Name: GenericNameSyntax { Identifier.ValueText: "Map", TypeArgumentList.Arguments.Count: 1 } },
			ArgumentList.Arguments.Count: 0,
		};

	static Site? Transform(GeneratorSyntaxContext ctx, CancellationToken ct)
	{
		var invocation = (InvocationExpressionSyntax)ctx.Node;
		if (ctx.SemanticModel.GetSymbolInfo(invocation, ct).Symbol is not IMethodSymbol method) return null;

		// An extension call binds to the REDUCED symbol (no receiver parameter): the definition is what it was reduced from (SG.probe A3).
		IMethodSymbol definition = method.ReducedFrom ?? method;
		if (definition.Name != "Map" || !definition.IsExtensionMethod || definition.Parameters.Length != 1) return null;
		if (ModelReader.FullName(definition.ContainingType) != "Open.Database.Extensions.CoreExtensions") return null;
		if (method.TypeArguments.Length != 1 || method.TypeArguments[0] is not INamedTypeSymbol model) return null; // A type parameter: not interceptable.

		ITypeSymbol receiver = definition.Parameters[0].Type;
		Receiver? kind = ModelReader.FullName(receiver) switch
		{
			"Open.Database.Extensions.IExecuteReader" => Receiver.ExecuteReader,
			"System.Data.IDbCommand" => Receiver.Command,
			"System.Data.IDataReader" => Receiver.DataReader,
			_ => null, // DataTable: its rows are not a DbDataReader; the runtime serves them.
		};
		if (kind is null) return null;

		ModelSpec? spec = ModelReader.Read(model, ctx.SemanticModel.Compilation);
		if (spec is null) return null;

		ct.ThrowIfCancellationRequested();
		if (ctx.SemanticModel.GetInterceptableLocation(invocation, ct) is not { } location) return null;

		return new Site(location.Version, location.Data, kind.Value, receiver.ToDisplayString(Fq), method.ReturnType.ToDisplayString(Fq), spec);
	}
}
