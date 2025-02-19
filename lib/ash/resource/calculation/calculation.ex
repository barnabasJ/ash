defmodule Ash.Resource.Calculation do
  @moduledoc "Represents a named calculation on a resource"

  defstruct [
    :name,
    :type,
    :calculation,
    :arguments,
    :description,
    :constraints,
    :private?,
    :allow_nil?,
    :select,
    :load,
    filterable?: true
  ]

  @schema [
    name: [
      type: :atom,
      required: true,
      doc: "The field name to use for the calculation value"
    ],
    type: [
      type: :any,
      doc: "The type of the calculation. See `Ash.Type` for more.",
      required: true
    ],
    constraints: [
      type: :keyword_list,
      default: [],
      doc: "Constraints to provide to the type. See `Ash.Type` for more."
    ],
    calculation: [
      type:
        {:or,
         [
           {:spark_function_behaviour, Ash.Calculation, {Ash.Calculation.Function, 2}},
           {:custom, __MODULE__, :expr_calc, []}
         ]},
      required: true,
      doc: """
      The module or `{module, opts}` to use for the calculation.
      Also accepts a function that takes a single record and produces the result.
      IMPORTANT: This function *does not take and return lists* like the `calculate/3` callback does.
      """
    ],
    description: [
      type: :string,
      doc: "An optional description for the calculation"
    ],
    private?: [
      type: :boolean,
      default: false,
      doc: """
      Whether or not the calculation will appear in any interfaces created off of this resource, e.g AshJsonApi and AshGraphql

      See the [security guide](/documentation/topics/security.md) for more.
      """
    ],
    select: [
      type: {:list, :atom},
      default: [],
      doc: "A list of fields to ensure selected if the calculation is used."
    ],
    load: [
      type: :any,
      default: [],
      doc: "A load statement to be applied if the calculation is used."
    ],
    allow_nil?: [
      type: :boolean,
      default: true,
      doc: "Whether or not the calculation can return nil."
    ],
    filterable?: [
      type: {:or, [:boolean, {:in, [:simple_equality]}]},
      default: true,
      doc: "Whether or not the calculation should be usable in filters."
    ]
  ]

  @type t :: %__MODULE__{
          name: atom(),
          calculation: {:ok, {atom(), any()}} | {:error, String.t()},
          arguments: list(any()),
          description: String.t() | nil,
          private?: boolean,
          allow_nil?: boolean
        }

  @type ref :: {module(), Keyword.t()} | module()

  defmodule Argument do
    @moduledoc "An argument to a calculation"
    defstruct [:name, :type, :default, :allow_nil?, :constraints, :allow_expr?]

    @schema [
      name: [
        type: :atom,
        required: true,
        doc: "The name of the argument"
      ],
      type: [
        type: Ash.OptionsHelpers.ash_type(),
        required: true,
        doc: "The type of the argument. See `Ash.Type` for more."
      ],
      default: [
        type: {:or, [{:mfa_or_fun, 0}, :literal]},
        required: false,
        doc: "A default value to use for the argument if not provided"
      ],
      allow_nil?: [
        type: :boolean,
        default: true,
        doc: "Whether or not the argument value may be nil (or may be not provided)"
      ],
      allow_expr?: [
        type: :boolean,
        default: false,
        doc: "Allow passing expressions as argument values. Expressions cannot be type validated."
      ],
      constraints: [
        type: :keyword_list,
        default: [],
        doc:
          "Constraints to provide to the type when casting the value. See the type's documentation and `Ash.Type` for more."
      ]
    ]

    def schema, do: @schema
  end

  def schema, do: @schema

  def expr_calc(expr) when is_function(expr) do
    {:error,
     "Inline function calculations expect a function with arity 2. Got #{Function.info(expr)[:arity]}"}
  end

  def expr_calc(expr) do
    {:ok, {Ash.Resource.Calculation.Expression, expr: expr}}
  end
end
