using Microsoft.CodeAnalysis;
using Microsoft.CodeAnalysis.CSharp;
using System.Reflection.Metadata;
using System.Reflection.PortableExecutable;
using System.Text.RegularExpressions;

namespace Open.Database.Extensions.Generator.Tests;

/// <summary>
/// The Debug.Assert the generator writes before each read of a non-nullable member costs a Release build nothing: compiled without DEBUG, the
/// generated file (this project's own, as the generator wrote it) gives the same IL, method by method, with and without the asserts; with DEBUG it
/// does not. Each compile is in memory (no file is written).
/// </summary>
public class GeneratedIlTests
{
	// One generated assert, as Emitter.Statement writes it, up to the space before the read it guards.
	static readonly Regex Assert_ = new(
		"""global::System\.Diagnostics\.Debug\.Assert\(.*?"' is NULL at this row\."\); """,
		RegexOptions.CultureInvariant);

	/// <summary>The generated file of this build (EmitCompilerGeneratedFiles), without its interceptor attributes (a compile here has no interceptors).</summary>
	static string GeneratedSource()
	{
		// bin/{Configuration}/{tfm}/ -> the project's obj/{Configuration}/{tfm}/generated/...
		var output = new DirectoryInfo(AppContext.BaseDirectory.TrimEnd(Path.DirectorySeparatorChar, Path.AltDirectorySeparatorChar));
		string tfm = output.Name;
		string configuration = output.Parent!.Name;
		string project = output.Parent.Parent!.Parent!.FullName;
		string path = Path.Combine(
			project, "obj", configuration, tfm, "generated",
			"Open.Database.Extensions.Generator", "Open.Database.Extensions.Generator.MapGenerator", "OdxMaps.g.cs");

		var lines = File.ReadAllLines(path)
			.Where(line => !line.TrimStart().StartsWith("[global::System.Runtime.CompilerServices.InterceptsLocation(", StringComparison.Ordinal));
		return string.Join("\n", lines);
	}

	static IEnumerable<MetadataReference> References()
	{
		// This process's assemblies: the runtime's, and (from this app's dependencies) Core and the models; each once.
		string platform = (string)AppContext.GetData("TRUSTED_PLATFORM_ASSEMBLIES")!;
		var paths = new HashSet<string>(platform.Split(Path.PathSeparator), StringComparer.OrdinalIgnoreCase)
		{
			typeof(RowMap<>).Assembly.Location,
			typeof(Models.PlainPost).Assembly.Location,
		};
		foreach (string path in paths)
			yield return MetadataReference.CreateFromFile(path);
	}

	/// <summary>Compiles the source, optimized, and returns each method's IL bytes by its declaring type and name (in metadata order).</summary>
	static List<KeyValuePair<string, byte[]>> CompileIl(string source, bool debugSymbol, MetadataReference[] references)
	{
		var parse = new CSharpParseOptions(LanguageVersion.Preview, preprocessorSymbols: debugSymbol ? ["DEBUG"] : []);
		var compilation = CSharpCompilation.Create(
			"OdxMapsIl",
			[CSharpSyntaxTree.ParseText(source, parse)],
			references,
			new CSharpCompilationOptions(OutputKind.DynamicallyLinkedLibrary, optimizationLevel: OptimizationLevel.Release, deterministic: true));

		using var image = new MemoryStream();
		var result = compilation.Emit(image);
		Assert.True(result.Success, string.Join("\n", result.Diagnostics.Where(d => d.Severity == DiagnosticSeverity.Error).Take(10)));

		image.Position = 0;
		using var pe = new PEReader(image);
		MetadataReader metadata = pe.GetMetadataReader();
		var methods = new List<KeyValuePair<string, byte[]>>();
		foreach (MethodDefinitionHandle handle in metadata.MethodDefinitions)
		{
			MethodDefinition method = metadata.GetMethodDefinition(handle);
			if (method.RelativeVirtualAddress == 0) continue;

			TypeDefinition type = metadata.GetTypeDefinition(method.GetDeclaringType());
			string name = metadata.GetString(type.Name) + "." + metadata.GetString(method.Name);
			methods.Add(new(name, pe.GetMethodBody(method.RelativeVirtualAddress).GetILBytes()!));
		}

		return methods;
	}

	[Fact]
	public void Release_IL_is_the_same_with_and_without_the_generated_asserts()
	{
		string source = GeneratedSource();
		string stripped = Assert_.Replace(source, "");
		int asserts = Assert_.Matches(source).Count;
		Assert.True(asserts > 100, $"Expected the generated asserts in the file, found {asserts}.");
		Assert.DoesNotContain("Debug.Assert", stripped, StringComparison.Ordinal);

		MetadataReference[] references = [.. References()];
		var release = CompileIl(source, debugSymbol: false, references);
		var releaseStripped = CompileIl(stripped, debugSymbol: false, references);

		Assert.Equal(releaseStripped.Select(m => m.Key), release.Select(m => m.Key));
		Assert.Contains(release, m => m.Key.EndsWith("GenRowT20.ReadLiteral", StringComparison.Ordinal));
		for (int i = 0; i < release.Count; i++)
			Assert.True(release[i].Value.AsSpan().SequenceEqual(releaseStripped[i].Value), $"{release[i].Key}: the IL differs.");

		// The probe works: with DEBUG the same source keeps the asserts, so the row readers' IL differs.
		var debug = CompileIl(source, debugSymbol: true, references);
		byte[] debugRead = debug.Single(m => m.Key.EndsWith("GenRowT20.ReadLiteral", StringComparison.Ordinal)).Value;
		byte[] releaseRead = release.Single(m => m.Key.EndsWith("GenRowT20.ReadLiteral", StringComparison.Ordinal)).Value;
		Assert.True(debugRead.Length > releaseRead.Length, "The DEBUG compile should keep the asserts.");
	}
}
