using System.Collections.Concurrent;
using System.Data;
using System.Data.Common;
using System.Linq.Expressions;
using System.Reflection;
using System.Reflection.Emit;
using FastExpressionCompiler;

namespace Open.Database.Extensions.Benchmarks;

// DS1b-F: can a tree built once match the static delegate? The same selector logic as V2's hand-written Read1..Read20, built four other ways (F0 to F3).
// F1, F2 and F3 read exactly what V2 reads: the typed getter for the member, IsDBNull only on the five nullable value columns and the three nullable
// string columns V2 tests, no try/catch, no index stores, the Blob through (byte[])GetValue(9), new SynRow() then the setters.
//   F0  the simple tier as it would ship: F1's tree (IDataRecord, typed getters, IsDBNull only on the nullable members, direct member assignment, no index stores)
//       plus ONE try/catch around the whole row. A local holds the ordinal of the cell being read, set before each cell; the catch resolves the column name from
//       the reader's schema (GetName, only there) and throws a ColumnReadException naming it. Same terminal as F1 and V2. F0 minus F1 = row-level error handling.
//   F1  an Expression<Func<IDataRecord, SynRow>> over the IDataRecord interface, Compile()d once in setup; used through V2's terminal (Map(selector).ToList()).
//   F2  the same tree with the reader parameter typed as the sealed SyntheticReader (direct calls); used through V9c-Syn's terminal (HandListSyn), where the
//       ONE cast (DbDataReader to SyntheticReader) sits: once per query, in the ExecuteReader callback, before the row loop. Nothing is cast per row or per cell.
//   F2u the F2 tree over UnsealedSyntheticReader, a copy of the synthetic reader class without `sealed` (SyntheticProviderUnsealed.cs); the same loop as F2, with
//       the one cast per query (DbDataReader to UnsealedSyntheticReader) in the ExecuteReader callback. F2 minus F2u = what the sealed class is worth.
//   F4  the F1 tree with the reader parameter typed as DbDataReader (virtual calls on the abstract class instead of interface calls); used through HandList (V9c-Db's
//       terminal), whose one cast per query sits in its ExecuteReader callback (the reader already is a DbDataReader; the cast is the loop's own, outside the row loop).
//   F3  the F1 logic as a static method of a real type emitted with Reflection.Emit into a collectible AssemblyBuilder (RunAndCollect), turned into a
//       delegate with CreateDelegate; used through V2's terminal. A real method on a real type, where Expression.Compile emits a DynamicMethod.
//   F3r F3's emitted method in a NON-collectible assembly (AssemblyBuilderAccess.Run); everything else as F3. F3r minus F3 = what collectibility costs.
//   F1f F1's tree compiled with FastExpressionCompiler.CompileFast (a different compiler than Expression.Compile; null on failure, which setup turns into an
//       error); V2's terminal. F1f minus F1 = the compiler, with the same tree.

/// <summary>What the three F variants run, built once per width: the compiled trees and the emitted method.</summary>
public sealed class FHands
{
	/// <summary>One member of the row: how V2 reads it. Check is true where V2 tests IsDBNull; Blob is the (byte[])GetValue(i) cell.</summary>
	sealed record Column(string Member, string Getter, bool Check = false, bool Blob = false);

	static readonly Column[] Columns =
	[
		new("Id", nameof(IDataRecord.GetInt32)),
		new("Created", nameof(IDataRecord.GetDateTime)),
		new("Title", nameof(IDataRecord.GetString)),
		new("Price", nameof(IDataRecord.GetDecimal)),
		new("Qty", nameof(IDataRecord.GetInt32), Check: true),
		new("IsActive", nameof(IDataRecord.GetBoolean)),
		new("Key", nameof(IDataRecord.GetGuid)),
		new("Score", nameof(IDataRecord.GetDouble)),
		new("Note", nameof(IDataRecord.GetString), Check: true),
		new("Blob", nameof(IDataRecord.GetValue), Blob: true),
		new("Big", nameof(IDataRecord.GetInt64)),
		new("Body", nameof(IDataRecord.GetString)),
		new("Amount", nameof(IDataRecord.GetDecimal), Check: true),
		new("Stamp", nameof(IDataRecord.GetDateTime), Check: true),
		new("Tag", nameof(IDataRecord.GetString)),
		new("Ratio", nameof(IDataRecord.GetDouble)),
		new("Flag", nameof(IDataRecord.GetBoolean), Check: true),
		new("Link", nameof(IDataRecord.GetGuid), Check: true),
		new("Count", nameof(IDataRecord.GetInt64)),
		new("Summary", nameof(IDataRecord.GetString), Check: true),
	];

	static readonly ConcurrentDictionary<int, FHands> Cache = new();

	// Keeps the emitted assemblies reachable for the life of the process (a delegate to a method of a collectible type already does; this makes it explicit).
	static readonly ConcurrentBag<AssemblyBuilder> Emitted = [];

	public required int Width { get; init; }

	/// <summary>F0: the simple tier, a compiled tree with one try/catch around the row.</summary>
	public required Func<IDataRecord, SynRow> F0 { get; init; }

	/// <summary>F1: the tree over IDataRecord, compiled.</summary>
	public required Func<IDataRecord, SynRow> F1 { get; init; }

	/// <summary>F2: the tree over the concrete reader, compiled.</summary>
	public required Func<SyntheticReader, SynRow> F2 { get; init; }

	/// <summary>F2u: the tree over the unsealed copy of the concrete reader, compiled.</summary>
	public required Func<UnsealedSyntheticReader, SynRow> F2u { get; init; }

	/// <summary>F4: the tree over DbDataReader, compiled.</summary>
	public required Func<DbDataReader, SynRow> F4 { get; init; }

	/// <summary>F3: the emitted static method, as a delegate over IDataRecord.</summary>
	public required Func<IDataRecord, SynRow> F3 { get; init; }

	/// <summary>F3r: the same emitted static method, in a NON-collectible (Run) assembly.</summary>
	public required Func<IDataRecord, SynRow> F3r { get; init; }

	/// <summary>F1f: F1's tree compiled with FastExpressionCompiler's CompileFast.</summary>
	public required Func<IDataRecord, SynRow> F1f { get; init; }

	/// <summary>The three for a width (any width 1 to 20), built on the first call and kept.</summary>
	public static FHands For(int width)
	{
		ArgumentOutOfRangeException.ThrowIfLessThan(width, 1);
		ArgumentOutOfRangeException.ThrowIfGreaterThan(width, Columns.Length);
		return Cache.GetOrAdd(width, static w => new()
		{
			Width = w,
			F0 = SimpleTree(w, -1).Compile(),
			F1 =Tree<IDataRecord>(w).Compile(),
			F2 = Tree<SyntheticReader>(w).Compile(),
			F2u = Tree<UnsealedSyntheticReader>(w).Compile(),
			F4 = Tree<DbDataReader>(w).Compile(),
			F3 = Emit(w, AssemblyBuilderAccess.RunAndCollect),
			F3r = Emit(w, AssemblyBuilderAccess.Run),
			F1f = Tree<IDataRecord>(w).CompileFast(ifFastFailedReturnNull: true)
				?? throw new InvalidOperationException($"FastExpressionCompiler could not compile the F1 tree at width {w}."),
		});
	}

	static MethodInfo Method<TReader>(string name)
		=> typeof(TReader).GetMethod(name, [typeof(int)])
			?? throw new InvalidOperationException($"{typeof(TReader).Name} has no {name}(int).");

	// new SynRow { Id = r.GetInt32(0), ..., Qty = r.IsDBNull(4) ? null : r.GetInt32(4), ..., Blob = (byte[])r.GetValue(9), ... } as an expression tree.
	static Expression<Func<TReader, SynRow>> Tree<TReader>(int width)
	{
		var r = Expression.Parameter(typeof(TReader), "r");
		var isDBNull = Method<TReader>(nameof(IDataRecord.IsDBNull));
		var bindings = new MemberBinding[width];
		for (int i = 0; i < width; i++)
		{
			var column = Columns[i];
			var property = typeof(SynRow).GetProperty(column.Member)!;
			Expression value = Expression.Call(r, Method<TReader>(column.Getter), Expression.Constant(i));
			if (column.Blob)
			{
				value = Expression.Convert(value, property.PropertyType);
			}
			else if (column.Check)
			{
				if (value.Type != property.PropertyType) value = Expression.Convert(value, property.PropertyType);
				value = Expression.Condition(
					Expression.Call(r, isDBNull, Expression.Constant(i)),
					Expression.Default(property.PropertyType),
					value);
			}

			bindings[i] = Expression.Bind(property, value);
		}

		return Expression.Lambda<Func<TReader, SynRow>>(
			Expression.MemberInit(Expression.New(typeof(SynRow)), bindings),
			r);
	}

	/// <summary>F0 with one cell planted bad (the getter at <paramref name="plantedOrdinal"/> is GetInt32, which a non-int column cannot satisfy), for the --verify column-name check.</summary>
	public static Func<IDataRecord, SynRow> PlantedBadCell(int width, int plantedOrdinal) => SimpleTree(width, plantedOrdinal).Compile();

	// F1's tree plus the row-level error handling:
	//   var row = new SynRow(); int col = 0;
	//   try { col = 0; row.Id = r.GetInt32(0); col = 1; row.Created = r.GetDateTime(1); ... } catch (Exception ex) { throw ColumnReadException.For(r, col, ex); }
	//   return row;
	// The name of the column is looked up inside the catch only. plantedOrdinal is -1 outside the --verify check.
	static Expression<Func<IDataRecord, SynRow>> SimpleTree(int width, int plantedOrdinal)
	{
		var r = Expression.Parameter(typeof(IDataRecord), "r");
		var row = Expression.Variable(typeof(SynRow), "row");
		var col = Expression.Variable(typeof(int), "col");
		var isDBNull = Method<IDataRecord>(nameof(IDataRecord.IsDBNull));
		var steps = new List<Expression>(2 * width);
		for (int i = 0; i < width; i++)
		{
			var column = Columns[i];
			var property = typeof(SynRow).GetProperty(column.Member)!;
			steps.Add(Expression.Assign(col, Expression.Constant(i)));
			if (i == plantedOrdinal)
			{
				// The planted cell: GetInt32 on a column that is not an int (the provider throws InvalidCastException); the value is not used.
				steps.Add(Expression.Call(r, Method<IDataRecord>(nameof(IDataRecord.GetInt32)), Expression.Constant(i)));
				continue;
			}

			Expression value = Expression.Call(r, Method<IDataRecord>(column.Getter), Expression.Constant(i));
			if (column.Blob)
			{
				value = Expression.Convert(value, property.PropertyType);
			}
			else if (column.Check)
			{
				if (value.Type != property.PropertyType) value = Expression.Convert(value, property.PropertyType);
				value = Expression.Condition(
					Expression.Call(r, isDBNull, Expression.Constant(i)),
					Expression.Default(property.PropertyType),
					value);
			}

			steps.Add(Expression.Assign(Expression.Property(row, property), value));
		}

		var ex = Expression.Parameter(typeof(Exception), "ex");
		var guarded = Expression.TryCatch(
			Expression.Block(typeof(void), steps),
			Expression.Catch(ex, Expression.Throw(Expression.Call(typeof(ColumnReadException).GetMethod(nameof(ColumnReadException.For))!, r, col, ex))));
		return Expression.Lambda<Func<IDataRecord, SynRow>>(
			Expression.Block([row, col], Expression.Assign(row, Expression.New(typeof(SynRow))), guarded, row),
			r);
	}

	// The same selector as IL: newobj, then for each member dup, the value, callvirt the setter (what the C# compiler emits for V2's Read<width>).
	static Func<IDataRecord, SynRow> Emit(int width, AssemblyBuilderAccess access)
	{
		var name = "OdxFEmit" + access + width;
		var assembly = AssemblyBuilder.DefineDynamicAssembly(new AssemblyName(name), access);
		Emitted.Add(assembly);
		var module = assembly.DefineDynamicModule(name);
		var type = module.DefineType("Readers", TypeAttributes.Public | TypeAttributes.Abstract | TypeAttributes.Sealed);
		var method = type.DefineMethod("Read", MethodAttributes.Public | MethodAttributes.Static, typeof(SynRow), [typeof(IDataRecord)]);
		var il = method.GetILGenerator();
		var isDBNull = Method<IDataRecord>(nameof(IDataRecord.IsDBNull));

		il.Emit(OpCodes.Newobj, typeof(SynRow).GetConstructor(Type.EmptyTypes)!);
		for (int i = 0; i < width; i++)
		{
			var column = Columns[i];
			var property = typeof(SynRow).GetProperty(column.Member)!;
			var getter = Method<IDataRecord>(column.Getter);
			var nullableValue = column.Check && property.PropertyType.IsValueType;

			il.Emit(OpCodes.Dup);
			if (column.Check)
			{
				var isNull = il.DefineLabel();
				var end = il.DefineLabel();
				il.Emit(OpCodes.Ldarg_0);
				il.Emit(OpCodes.Ldc_I4, i);
				il.Emit(OpCodes.Callvirt, isDBNull);
				il.Emit(OpCodes.Brtrue, isNull);
				il.Emit(OpCodes.Ldarg_0);
				il.Emit(OpCodes.Ldc_I4, i);
				il.Emit(OpCodes.Callvirt, getter);
				if (nullableValue) il.Emit(OpCodes.Newobj, property.PropertyType.GetConstructor([Nullable.GetUnderlyingType(property.PropertyType)!])!);
				il.Emit(OpCodes.Br, end);
				il.MarkLabel(isNull);
				if (nullableValue)
				{
					var local = il.DeclareLocal(property.PropertyType);
					il.Emit(OpCodes.Ldloca, local);
					il.Emit(OpCodes.Initobj, property.PropertyType);
					il.Emit(OpCodes.Ldloc, local);
				}
				else
				{
					il.Emit(OpCodes.Ldnull);
				}

				il.MarkLabel(end);
			}
			else
			{
				il.Emit(OpCodes.Ldarg_0);
				il.Emit(OpCodes.Ldc_I4, i);
				il.Emit(OpCodes.Callvirt, getter);
				if (column.Blob) il.Emit(OpCodes.Castclass, property.PropertyType);
			}

			il.Emit(OpCodes.Callvirt, property.SetMethod!);
		}

		il.Emit(OpCodes.Ret);
		var created = type.CreateType();
		return created.GetMethod("Read")!.CreateDelegate<Func<IDataRecord, SynRow>>();
	}
}

/// <summary>What F0 throws for a failing cell: private to the harness, names the column. The original exception is the inner exception.</summary>
public sealed class ColumnReadException(string column, int ordinal, Exception inner)
	: Exception($"Column {ordinal} ({column}) could not be read.", inner)
{
	public string Column { get; } = column;

	public int Ordinal { get; } = ordinal;

	/// <summary>Called inside F0's catch only: the name comes from the reader's schema.</summary>
	public static ColumnReadException For(IDataRecord record, int ordinal, Exception inner)
		=> new(record.GetName(ordinal), ordinal, inner);
}

public static class SyntheticVariantsF
{
	/// <summary>F0: V2's terminal with the simple tier (compiled tree, one try/catch around the row).</summary>
	public static List<SynRow> F0(SyntheticConnection connection, FHands hands) => SyntheticVariants.V2(connection, hands.F0);

	/// <summary>F1: V2's terminal with the compiled tree over IDataRecord.</summary>
	public static List<SynRow> F1(SyntheticConnection connection, FHands hands) => SyntheticVariants.V2(connection, hands.F1);

	/// <summary>F2: V9c-Syn's terminal (one cast to SyntheticReader per query, before the row loop) with the compiled tree over the concrete reader.</summary>
	public static List<SynRow> F2(SyntheticConnection connection, FHands hands) => SyntheticVariantsC.HandListSyn(connection, hands.F2);

	/// <summary>F2u: F2's loop over the unsealed copy of the provider path (one cast per query, before the row loop).</summary>
	public static List<SynRow> F2u(UnsealedSyntheticConnection connection, FHands hands)
		=> connection.Command(SyntheticVariants.Sql).ExecuteReader(hands.F2u, static (reader, read) =>
		{
			var db = (UnsealedSyntheticReader)reader;
			var list = new List<SynRow>();
			while (db.Read())
				list.Add(read(db));

			while (db.NextResult()) { }
			return list;
		}, CommandBehavior.SequentialAccess);

	/// <summary>F4: V9c-Db's terminal (HandList) with the tree over DbDataReader.</summary>
	public static List<SynRow> F4(SyntheticConnection connection, FHands hands) => SyntheticVariantsC.HandList(connection, hands.F4);

	/// <summary>F3: V2's terminal with the emitted static method.</summary>
	public static List<SynRow> F3(SyntheticConnection connection, FHands hands) => SyntheticVariants.V2(connection, hands.F3);

	/// <summary>F3r: V2's terminal with the method emitted into a non-collectible assembly.</summary>
	public static List<SynRow> F3r(SyntheticConnection connection, FHands hands) => SyntheticVariants.V2(connection, hands.F3r);

	/// <summary>F1f: V2's terminal with F1's tree compiled by FastExpressionCompiler.</summary>
	public static List<SynRow> F1f(SyntheticConnection connection, FHands hands) => SyntheticVariants.V2(connection, hands.F1f);
}
