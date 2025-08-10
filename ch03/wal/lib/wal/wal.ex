defmodule Wal.WAL do
  @moduledoc """
  Write-Ahead Log (WAL) implementation for distributed systems.

  This module provides a simple but robust WAL implementation that supports
  creating, writing, and reading log entries with persistent storage.

  ## Usage

      # Create and write entries
      entry1 = Wal.WAL.create_entry(1, "user:123:name:John Doe", :set)
      entry2 = Wal.WAL.create_entry(2, "user:124:email:jane@example.com", :set)

      :ok = Wal.WAL.write_entry(entry1, "/tmp/wal.log")
      :ok = Wal.WAL.write_entry(entry2, "/tmp/wal.log")

      # Read entries
      {:ok, entries} = Wal.WAL.read_entries("/tmp/wal.log")
      {:ok, entry} = Wal.WAL.read_entry("/tmp/wal.log", 1)

  ## Entry Types

  Currently supported entry types:
  - `:set` - Set operation (store data)

  ## Storage Format

  Entries are stored in binary format using Erlang Term Format for reliability
  and data integrity.
  """

  alias Wal.WalEntry

  @wal_file "wal.log"

  @type entry_type :: :set
  @type wal_entry :: WalEntry.t()

  @doc """
  Creates a new WAL entry.

  ## Parameters
  - `entry_index`: Unique index for the entry
  - `data`: Binary data to store
  - `entry_type`: Type of operation (currently only `:set`)
  - `timestamp`: Optional timestamp, defaults to current UTC time

  ## Examples

      iex> entry = Wal.WAL.create_entry(1, "test_data", :set)
      iex> entry.entry_index
      1
      iex> entry.entry_type
      :set

  """
  @spec create_entry(non_neg_integer(), binary(), entry_type(), DateTime.t()) :: wal_entry()
  def create_entry(entry_index, data, entry_type, timestamp \\ DateTime.utc_now()) do
    WalEntry.new(entry_index, data, entry_type, timestamp)
  end

  @doc """
  Writes a WAL entry to the specified file.

  The entry is serialized and stored with a size header for integrity.
  Multiple entries can be appended to the same file.

  ## Parameters
  - `entry`: The WAL entry to write

  ## Returns
  - `:ok` on success
  - `{:error, reason}` on failure

  ## Examples

      iex> entry = Wal.WAL.create_entry(1, "test_data", :set)
      iex> Wal.WAL.write_entry(entry)
      :ok

  """
  @spec write_entry(wal_entry()) :: :ok | {:error, term()}
  def write_entry(%WalEntry{} = entry) do
    serialized_entry = :erlang.term_to_binary(entry)
    entry_size = byte_size(serialized_entry)

    # Write entry size (4 bytes) followed by the serialized entry
    data_to_write = <<entry_size::32>> <> serialized_entry

    case File.open(@wal_file, [:write, :append, :binary]) do
      {:ok, file} ->
        result = IO.binwrite(file, data_to_write)
        File.close(file)
        result

      {:error, reason} ->
        {:error, reason}
    end
  end

  @doc """
  Writes multiple WAL entries to the specified file atomically.

  All entries are written in a single file operation for better performance
  and atomicity compared to multiple individual writes.

  ## Parameters
  - `entries`: List of WAL entries to write

  ## Returns
  - `:ok` on success
  - `{:error, reason}` on failure

  ## Examples

      iex> entries = [
      ...>   Wal.WAL.create_entry(1, "first", :set),
      ...>   Wal.WAL.create_entry(2, "second", :set)
      ...> ]
      iex> Wal.WAL.write_entries(entries)
      :ok
      iex> {:ok, read_entries} = Wal.WAL.read_entries()
      iex> length(read_entries)
      2
      iex> File.rm("wal.log")
      :ok

  """
  @spec write_entries([wal_entry()]) :: :ok | {:error, term()}
  def write_entries(entries) when is_list(entries) do
    case File.open(@wal_file, [:write, :append, :binary]) do
      {:ok, file} ->
        result = write_entries_to_file(entries, file)
        File.close(file)
        result

      {:error, reason} ->
        {:error, reason}
    end
  end

  # Private function to write multiple entries to an open file
  defp write_entries_to_file([], _file), do: :ok

  defp write_entries_to_file([entry | rest], file) do
    serialized_entry = :erlang.term_to_binary(entry)
    entry_size = byte_size(serialized_entry)
    data_to_write = <<entry_size::32>> <> serialized_entry

    case IO.binwrite(file, data_to_write) do
      :ok -> write_entries_to_file(rest, file)
      {:error, reason} -> {:error, reason}
    end
  end

  @doc """
  Reads all WAL entries from the specified file.

  Returns all entries stored in the file in the order they were written.

  ## Returns
  - `{:ok, [entries]}` on success
  - `{:error, reason}` on failure

  ## Examples

      iex> File.write!("wal.log", "")
      iex> {:ok, entries} = Wal.WAL.read_entries()
      iex> entries
      []
      iex> File.rm("wal.log")
      :ok

  """
  @spec read_entries() :: {:ok, [wal_entry()]} | {:error, term()}
  def read_entries() do
    case File.read(@wal_file) do
      {:ok, data} ->
        {:ok, parse_entries(data, [])}

      {:error, reason} ->
        {:error, reason}
    end
  end

  # Private function to parse binary data into WAL entries
  defp parse_entries(<<>>, acc), do: Enum.reverse(acc)

  defp parse_entries(<<entry_size::32, rest::binary>>, acc) when byte_size(rest) >= entry_size do
    <<entry_data::binary-size(entry_size), remaining::binary>> = rest

    try do
      entry = :erlang.binary_to_term(entry_data)
      parse_entries(remaining, [entry | acc])
    rescue
      _ ->
        # Skip corrupted entry and continue
        parse_entries(remaining, acc)
    end
  end

  defp parse_entries(_incomplete_data, acc), do: Enum.reverse(acc)

  @doc """
  Reads a single WAL entry by its index from the specified file.

  ## Parameters
  - `entry_index`: The index of the entry to read

  ## Returns
  - `{:ok, entry}` if found
  - `{:error, :not_found}` if not found
  - `{:error, reason}` on other failures

  ## Examples

      iex> entry = Wal.WAL.create_entry(1, "test_data", :set)
      iex> Wal.WAL.write_entry(entry)
      :ok
      iex> {:ok, read_entry} = Wal.WAL.read_entry(1)
      iex> read_entry.entry_index
      1
      iex> File.rm("wal.log")
      :ok

  """
  @spec read_entry(non_neg_integer()) :: {:ok, wal_entry()} | {:error, term()}
  def read_entry(entry_index) do
    case read_entries() do
      {:ok, entries} ->
        case Enum.find(entries, fn entry -> entry.entry_index == entry_index end) do
          nil -> {:error, :not_found}
          entry -> {:ok, entry}
        end

      {:error, reason} ->
        {:error, reason}
    end
  end

  @doc """
  Validates a WAL entry structure and type.

  ## Parameters
  - `entry`: The entry to validate

  ## Returns
  - `:ok` if valid
  - `{:error, reason}` if invalid

  ## Examples

      iex> entry = Wal.WAL.create_entry(1, "test_data", :set)
      iex> Wal.WAL.validate_entry(entry)
      :ok

  """
  @spec validate_entry(wal_entry()) :: :ok | {:error, term()}
  def validate_entry(%WalEntry{entry_type: :set} = _entry), do: :ok

  def validate_entry(%WalEntry{entry_type: invalid_type}),
    do: {:error, {:invalid_entry_type, invalid_type}}

  def validate_entry(_), do: {:error, :invalid_entry_struct}
end
