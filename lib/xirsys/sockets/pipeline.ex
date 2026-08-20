### ----------------------------------------------------------------------
###
### Copyright (c) 2013 - 2026 Jahred Love and Xirsys LLC <experts@xirsys.com>
###
### All rights reserved.
###
### XTurn is licensed by Xirsys under the Apache
### License, Version 2.0. (the "License");
###
### you may not use this file except in compliance with the License.
### You may obtain a copy of the License at
###
###      http://www.apache.org/licenses/LICENSE-2.0
###
### Unless required by applicable law or agreed to in writing, software
### distributed under the License is distributed on an "AS IS" BASIS,
### WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
### See the License for the specific language governing permissions and
### limitations under the License.
###
### See LICENSE for the full license text.
###
### ----------------------------------------------------------------------

defmodule Xirsys.Sockets.Pipeline do
  @moduledoc """
  Declarative multi-tier pipeline specification.

      use Xirsys.Sockets.Pipeline

      tier :root,
        accumulator: {Xirsys.Sockets.Accumulator.LengthPrefixed, header_size: 2},
        handler: MyApp.Handlers.Root

      tier :inner,
        accumulator: Xirsys.Sockets.Accumulator.Raw,
        handler: MyApp.Handlers.Inner,
        dispatch: :pool,
        pool_size: 8
  """

  alias Xirsys.Sockets.{Config, Conn, Pipeline.Tier, Spec}

  defstruct tiers: nil

  @type t :: %__MODULE__{tiers: %{atom() => Tier.t()}}

  @doc false
  defmacro __using__(_opts) do
    quote do
      @pipeline_tiers []
      import Xirsys.Sockets.Pipeline, only: [tier: 2]
      @before_compile Xirsys.Sockets.Pipeline
    end
  end

  @doc false
  defmacro tier(name, opts) do
    quote do
      @pipeline_tiers @pipeline_tiers ++ [unquote(name)]

      def __tier_spec__(unquote(name)) do
        tier_opts = unquote(opts)
        accumulator = Keyword.fetch!(tier_opts, :accumulator)
        handler = Keyword.fetch!(tier_opts, :handler)
        {accumulator_mod, accumulator_opts} = Spec.resolve(accumulator)

        %Tier{
          accumulator: accumulator_mod,
          accumulator_opts: accumulator_opts,
          handler: handler,
          dispatch: Keyword.get(tier_opts, :dispatch, :inline),
          pool_size: Keyword.get(tier_opts, :pool_size, nil),
          task_supervisor:
            Keyword.get(tier_opts, :task_supervisor, Xirsys.Sockets.TierSupervisor.Task),
          pool_supervisor:
            Keyword.get(tier_opts, :pool_supervisor, Xirsys.Sockets.TierSupervisor.Pool)
        }
      end
    end
  end

  @doc false
  defmacro __before_compile__(env) do
    tiers = Module.get_attribute(env.module, :pipeline_tiers)

    unless :root in tiers do
      raise CompileError,
            description: "pipeline must declare a :root tier",
            file: env.file,
            line: env.line
    end

    duplicates = tiers -- Enum.uniq(tiers)

    unless duplicates == [] do
      raise CompileError,
            description: "duplicate pipeline tier #{inspect(hd(duplicates))}",
            file: env.file,
            line: env.line
    end

    quote do
      def __tiers__, do: unquote(tiers)
    end
  end

  @doc """
  Resolves a compiled pipeline module or legacy `{accumulator, handler}` sugar
  into a runtime `%Pipeline{}` struct.
  """
  @spec resolve(module() | {Spec.spec(), module()}) :: t()
  def resolve(pipeline_mod) when is_atom(pipeline_mod) do
    tiers =
      pipeline_mod.__tiers__()
      |> Map.new(fn name ->
        tier = apply(pipeline_mod, :__tier_spec__, [name])
        {name, normalize_tier(tier)}
      end)

    %__MODULE__{tiers: tiers}
  end

  def resolve({accumulator_spec, handler_mod}) when is_atom(handler_mod) do
    {accumulator_mod, accumulator_opts} = Spec.resolve(accumulator_spec)

    %__MODULE__{
      tiers: %{
        root: %Tier{
          accumulator: accumulator_mod,
          accumulator_opts: accumulator_opts,
          handler: handler_mod,
          dispatch: :inline,
          pool_size: nil
        }
      }
    }
  end

  @doc """
  Returns the `%Pipeline.Tier{}` specification for a tier key.
  """
  @spec tier_spec(t(), atom()) :: Tier.t()
  def tier_spec(%__MODULE__{tiers: tiers}, key) do
    Map.fetch!(tiers, key)
  end

  @doc """
  Returns true when the pipeline declares more than the mandatory `:root` tier.
  """
  @spec multi_tier?(t()) :: boolean()
  def multi_tier?(%__MODULE__{tiers: tiers}), do: map_size(tiers) > 1

  @doc """
  Initializes root-tier accumulator and handler state maps for a connection.
  """
  @spec init_session(t(), Conn.t(), keyword()) :: {map(), map()}
  def init_session(%__MODULE__{} = pipeline, %Conn{} = conn, opts) do
    %Tier{accumulator: root_acc_mod, accumulator_opts: root_acc_opts, handler: root_handler} =
      tier_spec(pipeline, :root)

    default_handler_state = Keyword.get(opts, :handler_state, nil)

    root_state =
      if function_exported?(root_handler, :handle_connect, 1) do
        case root_handler.handle_connect(conn) do
          {:ok, state} -> state
          _ -> default_handler_state
        end
      else
        default_handler_state
      end

    accs = %{root: root_acc_mod.init(root_acc_opts)}
    states = %{root: root_state}
    {accs, states}
  end

  @doc """
  Builds fresh root-tier session maps (used for stateless UDP datagram handling).
  """
  @spec fresh_session(t(), keyword()) :: {map(), map()}
  def fresh_session(%__MODULE__{} = pipeline, opts) do
    %Tier{accumulator: root_acc_mod, accumulator_opts: root_acc_opts} =
      tier_spec(pipeline, :root)

    handler_state = Keyword.get(opts, :handler_state, nil)

    {%{root: root_acc_mod.init(root_acc_opts)}, %{root: handler_state}}
  end

  @spec normalize_tier(Tier.t()) :: Tier.t()
  defp normalize_tier(%Tier{dispatch: :pool, pool_size: nil} = tier) do
    %{tier | pool_size: Config.tier_pool_size()}
  end

  defp normalize_tier(%Tier{} = tier), do: tier
end
