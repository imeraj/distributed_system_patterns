defmodule Wal.WalEntry do
  @moduledoc false

  @type entry_type :: :set

  @type t :: %__MODULE__{
          entry_index: non_neg_integer(),
          data: binary(),
          entry_type: entry_type(),
          timestamp: DateTime.t()
        }

  defstruct [:entry_index, :data, :entry_type, :timestamp]

  @doc """
  Creates a new WAL entry with the given parameters.

  ## Parameters
  - `entry_index`: The index of the entry in the log
  - `data`: The data to be stored (binary)
  - `entry_type`: Currently only supports `:set`
  - `timestamp`: Optional timestamp, defaults to current UTC time

  ## Examples

      iex> entry = Wal.WalEntry.new(1, "some_data", :set)
      iex> entry.entry_index
      1
  """
  def new(entry_index, data, entry_type, timestamp \\ DateTime.utc_now()) do
    %__MODULE__{
      entry_index: entry_index,
      data: data,
      entry_type: entry_type,
      timestamp: timestamp
    }
  end
end
