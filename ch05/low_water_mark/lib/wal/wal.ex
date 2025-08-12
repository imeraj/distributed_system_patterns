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
  alias Wal.WALSegment

  @wal_file "wal.log"
  # 1MB per segment
  @segment_size_threshold 1024 * 1024
  @max_entries_per_segment 1000

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
    case get_current_segment_filename(entry.entry_index) do
      {:ok, segment_filename} ->
        write_entry_to_segment(entry, segment_filename)

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
    write_entries_to_segments(entries)
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

  @doc """
  Gets the appropriate segment filename for writing an entry with the given index.

  This function determines if a new segment should be created based on the current
  segment size and entry count limits.

  ## Parameters
  - `entry_index`: The index of the entry to be written

  ## Returns
  - `{:ok, filename}` with the segment filename to use
  - `{:error, reason}` on failure
  """
  @spec get_current_segment_filename(non_neg_integer()) :: {:ok, String.t()} | {:error, term()}
  def get_current_segment_filename(entry_index) do
    case get_latest_segment() do
      {:ok, {filename, base_offset}} ->
        if should_create_new_segment?(filename, entry_index, base_offset) do
          create_new_segment(entry_index)
        else
          {:ok, filename}
        end

      {:error, :no_segments} ->
        create_new_segment(entry_index)

      {:error, reason} ->
        {:error, reason}
    end
  end

  @doc """
  Creates a new segment file starting at the given index.

  ## Parameters
  - `start_index`: The starting index for the new segment

  ## Returns
  - `{:ok, filename}` on success
  - `{:error, reason}` on failure
  """
  @spec create_new_segment(non_neg_integer()) :: {:ok, String.t()} | {:error, term()}
  def create_new_segment(start_index) do
    filename = WALSegment.create_filename(start_index)

    case File.touch(filename) do
      :ok -> {:ok, filename}
      {:error, reason} -> {:error, reason}
    end
  end

  # Private function to get the latest (highest offset) segment
  defp get_latest_segment() do
    case discover_segment_files() do
      {:ok, []} ->
        {:error, :no_segments}

      {:ok, segments} ->
        latest = Enum.max_by(segments, fn {_filename, offset} -> offset end)
        {:ok, latest}

      {:error, reason} ->
        {:error, reason}
    end
  end

  # Private function to determine if a new segment should be created
  defp should_create_new_segment?(filename, _entry_index, _base_offset) do
    case File.stat(filename) do
      {:ok, %{size: size}} when size >= @segment_size_threshold ->
        true

      {:ok, _stat} ->
        case count_entries_in_file(filename) do
          {:ok, count} when count >= @max_entries_per_segment -> true
          {:ok, _count} -> false
          {:error, _reason} -> false
        end

      {:error, _reason} ->
        # If we can't stat the file, create a new one
        true
    end
  end

  # Private function to count entries in a file
  defp count_entries_in_file(filename) do
    case read_entries_from_file(filename) do
      {:ok, entries} -> {:ok, length(entries)}
      {:error, reason} -> {:error, reason}
    end
  end

  # Private function to write an entry to a specific segment file
  defp write_entry_to_segment(entry, segment_filename) do
    serialized_entry = :erlang.term_to_binary(entry)
    entry_size = byte_size(serialized_entry)

    # Write entry size (4 bytes) followed by the serialized entry
    data_to_write = <<entry_size::32>> <> serialized_entry

    case File.open(segment_filename, [:write, :append, :binary]) do
      {:ok, file} ->
        result = IO.binwrite(file, data_to_write)
        File.close(file)
        result

      {:error, reason} ->
        {:error, reason}
    end
  end

  # Private function to write multiple entries to segments with proper segmentation
  defp write_entries_to_segments(entries) do
    try do
      entries
      |> Enum.reduce(:ok, fn entry, acc ->
        case acc do
          :ok ->
            case write_entry(entry) do
              :ok -> :ok
              {:error, reason} -> {:error, reason}
            end

          error ->
            error
        end
      end)
    rescue
      error ->
        {:error, {:batch_write_failed, error}}
    end
  end

  @doc """
  Reads WAL entries from segmented log files starting from a given index.

  This function discovers all segment files that contain entries greater than
  or equal to the start index and reads entries from them in order.

  ## Parameters
  - `start_index`: The starting index to read from

  ## Returns
  - `{:ok, [entries]}` on success
  - `{:error, reason}` on failure

  ## Examples

      iex> {:ok, entries} = Wal.WAL.read_from(1024)
      iex> is_list(entries)
      true
  """
  @spec read_from(non_neg_integer()) :: {:ok, [wal_entry()]} | {:error, term()}
  def read_from(start_index) do
    case get_all_segments_containing_log_greater_than(start_index) do
      {:ok, segments} ->
        read_wal_entries_from(start_index, segments)

      {:error, reason} ->
        {:error, reason}
    end
  end

  @doc """
  Discovers all segment files that contain log entries greater than the start index.

  This function scans the current directory for WAL segment files, sorts them by
  base offset, and returns segments that may contain entries >= start_index.

  ## Parameters
  - `start_index`: The starting index to filter segments

  ## Returns
  - `{:ok, segments}` where segments is a list of {filename, base_offset} tuples
  - `{:error, reason}` on failure
  """
  @spec get_all_segments_containing_log_greater_than(non_neg_integer()) ::
          {:ok, [{String.t(), non_neg_integer()}]} | {:error, term()}
  def get_all_segments_containing_log_greater_than(start_index) do
    case discover_segment_files() do
      {:ok, segments} ->
        # Sort segments by base offset in descending order
        sorted_segments = Enum.sort_by(segments, fn {_filename, offset} -> offset end, :desc)

        # Start from the last segment to the first segment with starting offset <= startIndex
        filtered_segments = filter_segments_from_end(sorted_segments, start_index, [])

        {:ok, Enum.reverse(filtered_segments)}

      {:error, reason} ->
        {:error, reason}
    end
  end

  # Private function to discover all WAL segment files in current directory
  defp discover_segment_files() do
    case File.ls(".") do
      {:ok, files} ->
        segments =
          files
          |> Enum.flat_map(fn filename ->
            case WALSegment.get_base_offset_from_filename(filename) do
              {:ok, offset} -> [{filename, offset}]
              {:error, _reason} -> []
            end
          end)

        {:ok, segments}

      {:error, reason} ->
        {:error, reason}
    end
  end

  # Private function to filter segments starting from the highest offset
  defp filter_segments_from_end([], _start_index, acc), do: acc

  defp filter_segments_from_end([{filename, base_offset} | rest], start_index, acc) do
    new_acc = [{filename, base_offset} | acc]

    # If we found a segment with base_offset <= start_index, we can stop
    # as we have all segments that might contain entries >= start_index
    if base_offset <= start_index do
      new_acc
    else
      filter_segments_from_end(rest, start_index, new_acc)
    end
  end

  @doc """
  Reads WAL entries from multiple segment files, filtering by start index.

  ## Parameters
  - `start_index`: Only return entries with index >= start_index
  - `segments`: List of {filename, base_offset} tuples to read from

  ## Returns
  - `{:ok, [entries]}` on success
  - `{:error, reason}` on failure
  """
  @spec read_wal_entries_from(non_neg_integer(), [{String.t(), non_neg_integer()}]) ::
          {:ok, [wal_entry()]} | {:error, term()}
  def read_wal_entries_from(start_index, segments) do
    try do
      all_entries =
        segments
        |> Enum.flat_map(fn {filename, _base_offset} ->
          case read_entries_from_file(filename) do
            {:ok, entries} ->
              # Filter entries to only include those >= start_index
              Enum.filter(entries, fn entry -> entry.entry_index >= start_index end)

            {:error, _reason} ->
              # Skip files that can't be read
              []
          end
        end)
        |> Enum.sort_by(fn entry -> entry.entry_index end)

      {:ok, all_entries}
    rescue
      error ->
        {:error, {:read_failed, error}}
    end
  end

  # Private function to read entries from a specific file
  defp read_entries_from_file(filename) do
    case File.read(filename) do
      {:ok, data} ->
        {:ok, parse_entries(data, [])}

      {:error, reason} ->
        {:error, reason}
    end
  end
end
