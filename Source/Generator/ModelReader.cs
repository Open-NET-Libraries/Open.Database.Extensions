using Microsoft.CodeAnalysis;
using Microsoft.CodeAnalysis.CSharp;
using System.Collections.Immutable;
using System.Globalization;

namespace Open.Database.Extensions.Generator;

// Reads a model type the way the runtime RowMap<T> reads it with reflection (Source/Core/Core/RowMap.cs: Shape, BuildSlots, AddParameterSlots,
// NullAnnotations, Decoration, TryConvert), from symbols. A type it cannot describe EXACTLY is not intercepted (null): the call keeps running
// today's code, so a shape the runtime rejects still throws its own message at the call, and an unusual shape loses speed, never correctness.
static class ModelReader
{
	const string ColumnAttribute = "System.ComponentModel.DataAnnotations.Schema.ColumnAttribute";
	const string NotMappedAttribute = "System.ComponentModel.DataAnnotations.Schema.NotMappedAttribute";
	const string SetsRequiredMembersAttribute = "System.Diagnostics.CodeAnalysis.SetsRequiredMembersAttribute";
	const string AllowNullAttribute = "System.Diagnostics.CodeAnalysis.AllowNullAttribute";

	static readonly SymbolDisplayFormat Fq = SymbolDisplayFormat.FullyQualifiedFormat;

	public static ModelSpec? Read(INamedTypeSymbol t, Compilation compilation)
	{
		if (!IsMappableShape(t, compilation)) return null;

		// Shape.Choose: the constructor a model is built through.
		var constructors = t.InstanceConstructors
			.Where(c => c.DeclaredAccessibility == Accessibility.Public && !(t.IsValueType && c.IsImplicitlyDeclared))
			.ToList();
		IMethodSymbol? parameterless = constructors.FirstOrDefault(c => c.Parameters.Length == 0);
		IMethodSymbol? constructor = null;
		bool setsRequired;
		string create = "new " + t.ToDisplayString(Fq) + "()";
		if (parameterless is not null)
		{
			setsRequired = HasAttribute(parameterless.GetAttributes(), SetsRequiredMembersAttribute);
		}
		else if (t.IsValueType && constructors.Count == 0)
		{
			// The runtime creates a struct without a public constructor as its default value (Expression.New of a value type).
			setsRequired = false;
			create = "default(" + t.ToDisplayString(Fq) + ")";
		}
		else if (constructors.Count == 1)
		{
			constructor = constructors[0];
			foreach (IParameterSymbol p in constructor.Parameters)
				if (p.RefKind != RefKind.None || p.Type.TypeKind is TypeKind.Pointer or TypeKind.FunctionPointer) return null;

			setsRequired = HasAttribute(constructor.GetAttributes(), SetsRequiredMembersAttribute);
		}
		else
		{
			return null; // No public constructor, or several and none without parameters: the runtime throws at the call.
		}

		// A required public field cannot be set (Shape.RequiredField): the runtime throws at the call.
		if (!setsRequired)
		{
			for (INamedTypeSymbol? c = t; c is not null; c = c.BaseType)
			{
				foreach (ISymbol m in c.GetMembers())
					if (m is IFieldSymbol { IsStatic: false, DeclaredAccessibility: Accessibility.Public, IsRequired: true }) return null;
			}
		}

		List<IPropertySymbol>? properties = PublicProperties(t);
		if (properties is null) return null;

		var slots = new List<SlotSpec>();
		var seen = new HashSet<string>(StringComparer.Ordinal);

		// AddParameterSlots: a parameter and a public property whose names are equal (exactly, else ignoring case) are one slot, named after the property.
		if (constructor is not null)
		{
			ImmutableArray<IParameterSymbol> parameters = constructor.Parameters;
			var matches = new IPropertySymbol?[parameters.Length];
			bool[] taken = new bool[properties.Count];
			foreach (StringComparison comparison in new[] { StringComparison.Ordinal, StringComparison.OrdinalIgnoreCase })
			{
				for (int i = 0; i < parameters.Length; i++)
				{
					if (matches[i] is not null) continue;

					for (int p = 0; p < properties.Count; p++)
					{
						if (taken[p] || !string.Equals(properties[p].Name, parameters[i].Name, comparison)) continue;

						matches[i] = properties[p];
						taken[p] = true;
						break;
					}
				}
			}

			for (int i = 0; i < parameters.Length; i++)
			{
				IParameterSymbol parameter = parameters[i];
				IPropertySymbol? property = matches[i];
				string name = property?.Name ?? parameter.Name;
				seen.Add(name);

				Decoration declared = property is null ? Decoration.None : Decoration.Of(property);
				if (declared.NotMapped || declared.Invalid) return null; // [NotMapped] on a member a constructor parameter sets, or a declaration that is not valid: a type error at run time.

				// A required property the constructor also sets would have to be set again in an object initializer; the runtime sets it once. Not described here.
				bool required = property?.IsRequired == true && !setsRequired;
				if (required) return null;

				string? fallback = Fallback(parameter);
				if (fallback is null) return null;

				SlotSpec? slot = Slot(name, parameter.Name, i, parameter.Type, IsNonNullableReference(parameter), false, false, declared, fallback);
				if (slot is null) return null;
				slots.Add(slot);
			}
		}

		// BuildSlots: then every settable public property no parameter covers, in declaration order (a hidden one keeps the first).
		foreach (IPropertySymbol p in properties)
		{
			if (p.SetMethod is not { DeclaredAccessibility: Accessibility.Public } setter || !seen.Add(p.Name)) continue;

			var declared = Decoration.Of(p);
			bool required = p.IsRequired && !setsRequired;
			if (declared.Invalid || declared.NotMapped && (declared.HasColumnAttribute || required)) return null; // A type error at run time.

			SlotSpec? slot = Slot(p.Name, null, -1, p.Type, IsNonNullableReference(p), setter.IsInitOnly || required, required, declared, null);
			if (slot is null) return null;
			slots.Add(slot);
		}

		bool decorated = slots.Exists(s => s.Column is not null || s.Order >= 0);
		if (decorated && HasDuplicateDeclaration(slots)) return null; // A type error at run time.

		bool allOrdered = slots.Exists(s => !s.NotMapped) && slots.TrueForAll(s => s.NotMapped || s.Order >= 0);

		return new ModelSpec(
			t.ToDisplayString(Fq),
			constructor is not null,
			create,
			constructor?.Parameters.Length ?? 0,
			decorated,
			allOrdered,
			new EquatableArray<SlotSpec>([.. slots]));
	}

	// Shape.Judge, minus what the runtime would throw for (the call then runs today's code and throws there), plus what generated code cannot name.
	static bool IsMappableShape(INamedTypeSymbol t, Compilation compilation)
	{
		if (IsScalar(t) || t.IsTupleType || t.IsAnonymousType || t.IsRefLikeType || t.IsStatic) return false;
		if (t.OriginalDefinition.SpecialType == SpecialType.System_Nullable_T) return false;
		if (t.TypeKind is not (TypeKind.Class or TypeKind.Struct)) return false; // interface, delegate, enum, ...
		if (t.IsAbstract || ContainsTypeParameter(t)) return false;
		if (t.IsFileLocal || !compilation.IsSymbolAccessibleWithin(t, compilation.Assembly)) return false;

		foreach (INamedTypeSymbol i in t.AllInterfaces)
		{
			string name = FullName(i.OriginalDefinition);
			if (name is "System.Collections.IDictionary" or "System.Dynamic.IDynamicMetaObjectProvider"
				or "System.Collections.Generic.IDictionary`2" or "System.Collections.Generic.IReadOnlyDictionary`2")
			{
				return false;
			}
		}

		return true;
	}

	// Shape.IsScalar: a single value is read from the first column, not mapped by name.
	static bool IsScalar(ITypeSymbol t)
	{
		if (t.TypeKind == TypeKind.Enum) return true;
		return t.SpecialType switch
		{
			SpecialType.System_Boolean or SpecialType.System_Char or SpecialType.System_SByte or SpecialType.System_Byte or SpecialType.System_Int16 or SpecialType.System_UInt16 or SpecialType.System_Int32 or SpecialType.System_UInt32 or SpecialType.System_Int64 or SpecialType.System_UInt64 or SpecialType.System_Single or SpecialType.System_Double or SpecialType.System_IntPtr or SpecialType.System_UIntPtr or SpecialType.System_String or SpecialType.System_Decimal or SpecialType.System_DateTime => true,
			_ => FullName(t) is "System.DateTimeOffset" or "System.TimeSpan" or "System.Guid" or "System.DateOnly" or "System.TimeOnly",
		};
	}

	static bool ContainsTypeParameter(ITypeSymbol t) => t switch
	{
		ITypeParameterSymbol => true,
		IArrayTypeSymbol a => ContainsTypeParameter(a.ElementType),
		INamedTypeSymbol n => n.TypeArguments.Any(ContainsTypeParameter) || n.ContainingType is { } c && ContainsTypeParameter(c),
		_ => false,
	};

	// typeof(T).GetProperties(Public | Instance) without indexers: the type's own, then each base type's, in declaration order.
	// A name declared again further up (an override, a `new`) is filtered by reflection rules this does not reproduce: null, not intercepted.
	static List<IPropertySymbol>? PublicProperties(INamedTypeSymbol t)
	{
		var list = new List<IPropertySymbol>();
		var names = new HashSet<string>(StringComparer.Ordinal);
		for (INamedTypeSymbol? c = t; c is not null; c = c.BaseType)
		{
			foreach (ISymbol m in c.GetMembers())
			{
				if (m is not IPropertySymbol { IsStatic: false, IsIndexer: false, DeclaredAccessibility: Accessibility.Public } p) continue;
				if (p.IsOverride || p.ReturnsByRef || p.ReturnsByRefReadonly || !names.Add(p.Name)) return null;
				if (p.Type.TypeKind is TypeKind.Pointer or TypeKind.FunctionPointer) return null;
				list.Add(p);
			}
		}

		return list;
	}

	static SlotSpec? Slot(string name, string? parameter, int position, ITypeSymbol type, bool nonNullableReference, bool initBound, bool required, Decoration declared, string? fallback)
	{
		bool nullableValue = type.OriginalDefinition.SpecialType == SpecialType.System_Nullable_T;
		ITypeSymbol underlying = nullableValue ? ((INamedTypeSymbol)type).TypeArguments[0] : type;

		// RowMap's Slot.KeepsValueOnNull test: a value type other than Nullable<T>, or a reference type the source declares non-nullable (oblivious:
		// nullable). Read without a NULL test, except object or an interface: GetValue's DBNull would pass its cast silently, so those stay tested.
		bool nonNullable = type.IsValueType ? !nullableValue : nonNullableReference && type.SpecialType != SpecialType.System_Object && type.TypeKind != TypeKind.Interface;

		ImmutableArray<ReadCase> cases = Cases(underlying);
		if (cases.IsEmpty) return null;

		return new SlotSpec(
			name, parameter, position,
			type.ToDisplayString(Fq), underlying.ToDisplayString(Fq), nullableValue,
			nonNullable, initBound, required,
			declared.Column, declared.Order, declared.NotMapped,
			fallback,
			new EquatableArray<ReadCase>(cases));
	}

	// RowMap.TypedGetters, in the order the reads are offered when no exact read exists.
	static readonly (SpecialType Type, string Getter)[] Getters =
	[
		(SpecialType.System_Boolean, "GetBoolean"),
		(SpecialType.System_Byte, "GetByte"),
		(SpecialType.System_Char, "GetChar"),
		(SpecialType.System_Int16, "GetInt16"),
		(SpecialType.System_Int32, "GetInt32"),
		(SpecialType.System_Int64, "GetInt64"),
		(SpecialType.System_Single, "GetFloat"),
		(SpecialType.System_Double, "GetDouble"),
		(SpecialType.System_Decimal, "GetDecimal"),
		(SpecialType.System_DateTime, "GetDateTime"),
		(SpecialType.None, "GetGuid"), // System.Guid has no SpecialType: matched by name.
		(SpecialType.System_String, "GetString"),
	];

	static string TypeOf(SpecialType s) => s switch
	{
		SpecialType.System_Boolean => "global::System.Boolean",
		SpecialType.System_Byte => "global::System.Byte",
		SpecialType.System_Char => "global::System.Char",
		SpecialType.System_Int16 => "global::System.Int16",
		SpecialType.System_Int32 => "global::System.Int32",
		SpecialType.System_Int64 => "global::System.Int64",
		SpecialType.System_Single => "global::System.Single",
		SpecialType.System_Double => "global::System.Double",
		SpecialType.System_Decimal => "global::System.Decimal",
		SpecialType.System_DateTime => "global::System.DateTime",
		SpecialType.System_String => "global::System.String",
		_ => "global::System.Guid",
	};

	static bool IsIntegral(SpecialType s) => s is SpecialType.System_Byte or SpecialType.System_SByte or SpecialType.System_Int16 or SpecialType.System_UInt16
		or SpecialType.System_Int32 or SpecialType.System_UInt32 or SpecialType.System_Int64 or SpecialType.System_UInt64;

	static bool IsFractional(SpecialType s) => s is SpecialType.System_Single or SpecialType.System_Double or SpecialType.System_Decimal;

	// RowMap.TypedRead + TryConvert, per column type a getter exists for; then, for a member type that has no getter of its own,
	// the general path's exact case (ConvertValue returns a value that already is the type as it is). Anything else is the general path: not generated.
	static ImmutableArray<ReadCase> Cases(ITypeSymbol target)
	{
		string targetFq = target.ToDisplayString(Fq);
		bool targetIsGuid = target.SpecialType == SpecialType.None && FullName(target) == "System.Guid";
		SpecialType targetSpecial = targetIsGuid ? SpecialType.None : target.SpecialType;
		bool targetHasGetter = targetIsGuid || targetSpecial != SpecialType.None && Getters.Any(g => g.Type == targetSpecial);
		SpecialType t = target is INamedTypeSymbol { TypeKind: TypeKind.Enum, EnumUnderlyingType: { } u } ? u.SpecialType : targetSpecial;
		bool isEnum = target.TypeKind == TypeKind.Enum;

		var exact = new List<ReadCase>();
		var others = new List<ReadCase>();
		foreach ((SpecialType source, string getter) in Getters)
		{
			string read = "r." + getter + "({0})";
			string column = TypeOf(source);
			bool sourceIsGuid = getter == "GetGuid";
			string? expression = null;

			if (targetHasGetter && (sourceIsGuid ? targetIsGuid : !targetIsGuid && source == targetSpecial))
				expression = read;
			else if (sourceIsGuid || targetIsGuid)
				expression = null;
			else if (targetSpecial == SpecialType.System_Boolean && IsIntegral(source))
				expression = read + " != 0";
			else if (source == SpecialType.System_String && targetSpecial == SpecialType.System_DateTime)
				expression = "global::System.DateTime.Parse(" + read + ", global::System.Globalization.CultureInfo.InvariantCulture)";
			else if (source == SpecialType.System_String && targetSpecial == SpecialType.System_Decimal)
				expression = "global::System.Decimal.Parse(" + read + ", global::System.Globalization.NumberStyles.Number, global::System.Globalization.CultureInfo.InvariantCulture)";
			else if (IsIntegral(source) && IsIntegral(t))
				expression = "checked((" + targetFq + ")" + read + ")";
			else if ((IsIntegral(source) || IsFractional(source)) && IsFractional(t) && !isEnum)
				expression = "(" + targetFq + ")" + read;

			if (expression is null) continue;

			var c = new ReadCase(column, expression, false);
			// The read Full mode takes: the column's own type, or for an enum the column of its underlying type.
			if (expression == read || isEnum && source == t) exact.Add(c);
			else others.Add(c);
		}

		if (!targetHasGetter && target.TypeKind != TypeKind.TypeParameter)
		{
			var general = new ReadCase(targetFq, "(" + targetFq + "){0}", true);
			if (exact.Count == 0) exact.Add(general);
			else others.Add(general);
		}

		return [.. exact, .. others];
	}

	// NullAnnotations.IsNonNullableReference(PropertyInfo): a reference type with nullable annotations on, not declared as a type parameter, no [AllowNull].
	static bool IsNonNullableReference(IPropertySymbol p)
	{
		IPropertySymbol declared = p.OriginalDefinition;
		if (!IsReference(p.Type) || declared.Type is ITypeParameterSymbol) return false;
		if (HasAttribute(p.GetAttributes(), AllowNullAttribute)) return false;
		if (p.SetMethod is { Parameters: { Length: > 0 } values } && HasAttribute(values[values.Length - 1].GetAttributes(), AllowNullAttribute)) return false;
		return declared.NullableAnnotation == NullableAnnotation.NotAnnotated;
	}

	// NullAnnotations.IsNonNullableReference(ParameterInfo): the same, and [AllowNull] is not read for a parameter.
	static bool IsNonNullableReference(IParameterSymbol p)
	{
		IParameterSymbol declared = p.OriginalDefinition;
		if (!IsReference(p.Type) || declared.Type is ITypeParameterSymbol) return false;
		return declared.NullableAnnotation == NullableAnnotation.NotAnnotated;
	}

	static bool IsReference(ITypeSymbol t) => !t.IsValueType && t.TypeKind is not (TypeKind.TypeParameter or TypeKind.Pointer or TypeKind.FunctionPointer);

	// RowMap.DuplicateDeclaration: two members declaring one ordinal, or one column name ignoring case.
	static bool HasDuplicateDeclaration(List<SlotSpec> slots)
	{
		for (int j = 1; j < slots.Count; j++)
		{
			for (int i = 0; i < j; i++)
			{
				if (slots[i].Order >= 0 && slots[i].Order == slots[j].Order) return true;
				if (slots[i].Column is { } c && string.Equals(c, slots[j].Column, StringComparison.OrdinalIgnoreCase)) return true;
			}
		}

		return false;
	}

	// Slot.FallbackOf: the declared default of a parameter, else default(TParam), as a C# expression; null for a default it does not write (the model is then not intercepted).
	static string? Fallback(IParameterSymbol p)
	{
		string type = p.Type.ToDisplayString(Fq);
		if (!p.HasExplicitDefaultValue || p.ExplicitDefaultValue is null) return "default(" + type + ")";

		object value = p.ExplicitDefaultValue;
		bool nullable = p.Type.OriginalDefinition.SpecialType == SpecialType.System_Nullable_T;
		ITypeSymbol underlying = nullable ? ((INamedTypeSymbol)p.Type).TypeArguments[0] : p.Type;
		string? literal = Literal(value);
		if (literal is null) return null;

		if (underlying.TypeKind == TypeKind.Enum)
		{
			string e = underlying.ToDisplayString(Fq);
			return nullable ? "(" + type + ")(" + e + ")(" + literal + ")" : "(" + e + ")(" + literal + ")";
		}

		return "(" + type + ")(" + literal + ")";
	}

	static string? Literal(object value) => value switch
	{
		bool b => b ? "true" : "false",
		string s => SymbolDisplay.FormatLiteral(s, true),
		char c => SymbolDisplay.FormatLiteral(c, true),
		int i => i.ToString(CultureInfo.InvariantCulture),
		uint u => u.ToString(CultureInfo.InvariantCulture) + "U",
		long l => l.ToString(CultureInfo.InvariantCulture) + "L",
		ulong u => u.ToString(CultureInfo.InvariantCulture) + "UL",
		short s => "(global::System.Int16)(" + s.ToString(CultureInfo.InvariantCulture) + ")",
		ushort s => "(global::System.UInt16)(" + s.ToString(CultureInfo.InvariantCulture) + ")",
		byte b => "(global::System.Byte)(" + b.ToString(CultureInfo.InvariantCulture) + ")",
		sbyte b => "(global::System.SByte)(" + b.ToString(CultureInfo.InvariantCulture) + ")",
		float f when float.IsNaN(f) => "global::System.Single.NaN",
		float f when float.IsPositiveInfinity(f) => "global::System.Single.PositiveInfinity",
		float f when float.IsNegativeInfinity(f) => "global::System.Single.NegativeInfinity",
		float f => f.ToString("R", CultureInfo.InvariantCulture) + "F",
		double d when double.IsNaN(d) => "global::System.Double.NaN",
		double d when double.IsPositiveInfinity(d) => "global::System.Double.PositiveInfinity",
		double d when double.IsNegativeInfinity(d) => "global::System.Double.NegativeInfinity",
		double d => d.ToString("R", CultureInfo.InvariantCulture) + "D",
		decimal m => m.ToString(CultureInfo.InvariantCulture) + "M",
		_ => null,
	};

	static bool HasAttribute(ImmutableArray<AttributeData> attributes, string fullName)
	{
		foreach (AttributeData a in attributes)
			if (a.AttributeClass is { } c && FullName(c) == fullName) return true;

		return false;
	}

	// The metadata full name (namespace + nested types + arity), as reflection's Type.FullName gives it for a non-generic or definition type.
	public static string FullName(ITypeSymbol t)
	{
		string name = t.MetadataName;
		for (INamedTypeSymbol? c = t.ContainingType; c is not null; c = c.ContainingType) name = c.MetadataName + "+" + name;
		INamespaceSymbol? ns = t.ContainingNamespace;
		return ns?.IsGlobalNamespace != false ? name : $"{ns.ToDisplayString()}.{name}";
	}

	// RowMap's Decoration: read as data, by full name.
	readonly struct Decoration(string? column, int order, bool hasColumnAttribute, bool notMapped, bool invalid)
	{
		public string? Column => column;
		public int Order => order;
		public bool HasColumnAttribute => hasColumnAttribute;
		public bool NotMapped => notMapped;

		// An empty or white-space name, or a negative ordinal: the runtime makes the type unmappable (a declaration is a hard rule), so the call is left to it.
		public bool Invalid => invalid;

		public static Decoration None => new(null, -1, false, false, false);

		public static Decoration Of(IPropertySymbol property)
		{
			string? column = null;
			int order = -1;
			bool hasColumn = false, notMapped = false, invalid = false;
			foreach (AttributeData a in property.GetAttributes())
			{
				if (a.AttributeClass is not { } c) continue;

				string name = FullName(c);
				if (name == NotMappedAttribute)
				{
					notMapped = true;
				}
				else if (name == ColumnAttribute)
				{
					hasColumn = true;
					if (a.ConstructorArguments.Length > 0)
					{
						if (a.ConstructorArguments[0].Value is string given && !string.IsNullOrWhiteSpace(given)) column = given;
						else invalid = true;
					}

					foreach (KeyValuePair<string, TypedConstant> named in a.NamedArguments)
					{
						if (named.Key != "Order" || named.Value.Value is not int value) continue;
						if (value >= 0) order = value;
						else invalid = true;
					}
				}
			}

			return new(column, order, hasColumn, notMapped, invalid);
		}
	}
}
