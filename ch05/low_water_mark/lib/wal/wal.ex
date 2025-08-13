defmodule Wal.WAL do
  use GenServer

  @moduledoc """
  GenServer-based Write-Ahead Log (WAL) implementation for distributed systems.

  This module provides a simple but robust WAL implementation that supports
  creating, writing, and reading log entries with persistent storage.

  ## Usage

      # Start the WAL GenServer
      {:ok, wal_pid} = Wal.WAL.start_link([])

      # Create and write entries
      entry1 = Wal.WAL.create_entry(1, "user:123:name:John Doe", :set)
      entry2 = Wal.WAL.create_entry(2, "user:124:email:jane@example.com", :set)

      :ok = Wal.WAL.write_entry(wal_pid, entry1)
      :ok = Wal.WAL.write_entry(wal_pid, entry2)

      # Read entries
      {:ok, entries} = Wal.WAL.read_entries(wal_pid)
      {:ok, entry} = Wal.WAL.read_entry(wal_pid, 1)

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

  ## Client API

  @doc """
  Starts the WAL GenServer.

  ## Options
  - `:name` - Name to register the GenServer (optional)
  """
  def start_link(opts \\ []) do
    name = Keyword.get(opts, :name, __MODULE__)
    GenServer.start_link(__MODULE__, opts, name: name)
  end

  @doc """
  Creates a new WAL entry (pure function).

  ## Parameters
  - `entry_index`: Unique index for the entry
  - `data`: Binary data to store
  - `entry_type`: Type of operation (currently only `:set`)
  - `timestamp`: Optional timestamp, defaults to current UTC time
  """
  def create_entry(entry_index, data, entry_type, timestamp \\ DateTime.utc_now()) do
    WalEntry.new(entry_index, data, entry_type, timestamp)
  end

  @doc """
  Writes a WAL entry to the appropriate segment file.

  ## Parameters
  - `server`: WAL GenServer pid or name
  - `entry`: The WAL entry to write
  """
  def write_entry(server, %WalEntry{} = entry) do
    GenServer.call(server, {:write_entry, entry})
  end

  @doc """
  Writes multiple WAL entries atomically.

  ## Parameters
  - `server`: WAL GenServer pid or name
  - `entries`: List of WAL entries to write
  """
  def write_entries(server, entries) when is_list(entries) do
    GenServer.call(server, {:write_entries, entries})
  end

  @doc """
  Reads all WAL entries from all segments.

  ## Parameters
  - `server`: WAL GenServer pid or name
  """
  def read_entries(server) do
    GenServer.call(server, :read_entries)
  end

  @doc """
  Reads a single WAL entry by its index.

  ## Parameters
  - `server`: WAL GenServer pid or name
  - `entry_index`: The index of the entry to read
  """
  def read_entry(server, entry_index) do
    GenServer.call(server, {:read_entry, entry_index})
  end

  @doc """
  Reads WAL entries from segmented log files starting from a given index.

  ## Parameters
  - `server`: WAL GenServer pid or name
  - `start_index`: The starting index to read from
  """
  def read_from(server, start_index) do
    GenServer.call(server, {:read_from, start_index})
  end

  @doc """
  Gets information about WAL segments currently in use.

  ## Parameters
  - `server`: WAL GenServer pid or name
  """
  def get_all_segments_containing_log_greater_than(server, start_index) do
    GenServer.call(server, {:get_all_segments_containing_log_greater_than, start_index})
  end

  @doc """
  Gets the current active segment filename being written to.

  ## Parameters
  - `server`: WAL GenServer pid or name
  - `entry_index`: The index for which to get the segment filename
  """
  def get_current_segment_filename(server, entry_index) do
    GenServer.call(server, {:get_current_segment_filename, entry_index})
  end

  @doc """
  Validates a WAL entry structure and type (pure function).
  """
  def validate_entry(%WalEntry{entry_type: :set}), do: :ok

  def validate_entry(%WalEntry{entry_type: invalid_type}),
    do: {:error, {:invalid_entry_type, invalid_type}}

  def validate_entry(_), do: {:error, :invalid_entry_struct}

  ## GenServer Implementation

  @impl true
  def init(_opts) do
    # Initial state: no cached state needed.
    # Discover all segment files on startup and sort them
    case discover_segment_files() do
      {:ok, segments} ->
        sorted_segments = Enum.sort_by(segments, fn {_filename, offset} -> offset end)
        state = %{sorted_saved_segments: sorted_segments}
        {:ok, state}

      {:error, _reason} ->
        state = %{sorted_saved_segments: []}
        {:ok, state}
    end
  end

  @impl true
  def handle_call({:write_entry, entry}, _from, state) do
    case get_current_segment_filename_impl(entry.entry_index, state) do
      {:ok, segment_filename, new_state} ->
        result = write_entry_to_segment(entry, segment_filename)
        {:reply, result, new_state}

      {:error, reason, new_state} ->
        {:reply, {:error, reason}, new_state}
    end
  end

  @impl true
  def handle_call({:write_entries, entries}, _from, state) do
    result = write_entries_to_segments(entries, state)
    {:reply, result, state}
  end

  @impl true
  def handle_call(:read_entries, _from, state) do
    case File.read(@wal_file) do
      {:ok, data} ->
        {:reply, {:ok, parse_entries(data, [])}, state}

      {:error, reason} ->
        {:reply, {:error, reason}, state}
    end
  end

  @impl true
  def handle_call({:read_entry, entry_index}, _from, state) do
    case File.read(@wal_file) do
      {:ok, data} ->
        entries = parse_entries(data, [])

        case Enum.find(entries, fn entry -> entry.entry_index == entry_index end) do
          nil -> {:reply, {:error, :not_found}, state}
          entry -> {:reply, {:ok, entry}, state}
        end

      {:error, reason} ->
        {:reply, {:error, reason}, state}
    end
  end

  @impl true
  def handle_call({:read_from, start_index}, _from, state) do
    case get_all_segments_containing_log_greater_than_impl(start_index) do
      {:ok, segments} ->
        result = read_wal_entries_from(start_index, segments)
        {:reply, result, state}

      {:error, reason} ->
        {:reply, {:error, reason}, state}
    end
  end

  @impl true
  def handle_call({:get_all_segments_containing_log_greater_than, start_index}, _from, state) do
    result = get_all_segments_containing_log_greater_than_impl(start_index)
    {:reply, result, state}
  end

  @impl true
  def handle_call({:get_current_segment_filename, entry_index}, _from, state) do
    result = get_current_segment_filename_impl(entry_index, state)
    {:reply, result, state}
  end

  ## Private Implementation

  # Parse binary data into WAL entries
  defp parse_entries(<<>>, acc), do: Enum.reverse(acc)

  defp parse_entries(<<entry_size::32, rest::binary>>, acc) when byte_size(rest) >= entry_size do
    <<entry_data::binary-size(entry_size), remaining::binary>> = rest

    try do
      entry = :erlang.binary_to_term(entry_data)
      parse_entries(remaining, [entry | acc])
    rescue
      _ -> parse_entries(remaining, acc)
    end
  end

  defp parse_entries(_incomplete_data, acc), do: Enum.reverse(acc)

  # Write entry to segment file
  defp write_entry_to_segment(entry, segment_filename) do
    serialized_entry = :erlang.term_to_binary(entry)
    entry_size = byte_size(serialized_entry)
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

  # Write multiple entries to segments
  defp write_entries_to_segments(entries, state) do
    try do
      entries
      |> Enum.reduce({:ok, state}, fn entry, {result, curr_state} ->
        case result do
          :ok ->
            case get_current_segment_filename_impl(entry.entry_index, curr_state) do
              {:ok, segment_filename, new_state} ->
                case write_entry_to_segment(entry, segment_filename) do
                  :ok -> {:ok, new_state}
                  {:error, reason} -> {{:error, reason}, new_state}
                end

              {:error, reason, new_state} ->
                {{:error, reason}, new_state}
            end

          {:error, reason} ->
            {{:error, reason}, curr_state}
        end
      end)
      |> (fn
            {:ok, _final_state} -> :ok
            {{:error, reason}, _final_state} -> {:error, reason}
          end).()
    rescue
      error -> {:error, {:batch_write_failed, error}}
    end
  end

  # Get current segment filename for entry index
  defp get_current_segment_filename_impl(entry_index, state) do
    case get_latest_segment(state) do
      {:ok, {filename, base_offset}} ->
        if should_create_new_segment?(filename, entry_index, base_offset) do
          case create_new_segment(entry_index, state) do
            {:ok, new_filename, new_state} -> {:ok, new_filename, new_state}
            {:error, reason, _new_state} -> {:error, reason, state}
          end
        else
          {:ok, filename, state}
        end

      {:error, :no_segments} ->
        case create_new_segment(entry_index, state) do
          {:ok, new_filename, new_state} -> {:ok, new_filename, new_state}
          {:error, reason, _new_state} -> {:error, reason, state}
        end

      {:error, reason} ->
        {:error, reason, state}
    end
  end

  # Create new segment file
  defp create_new_segment(start_index, state) do
    filename = WALSegment.create_filename(start_index)

    case File.touch(filename) do
      :ok ->
        # Update sorted_saved_segments in state
        new_segment = {filename, start_index}

        sorted_segments =
          Enum.sort_by([new_segment | state.sorted_saved_segments], fn {_f, offset} -> offset end)

        {:ok, filename, %{state | sorted_saved_segments: sorted_segments}}

      {:error, reason} ->
        {:error, reason, state}
    end
  end

  # Get latest segment file
  defp get_latest_segment(state) do
    case state.sorted_saved_segments do
      [] ->
        {:error, :no_segments}

      segments ->
        latest = Enum.max_by(segments, fn {_filename, offset} -> offset end)
        {:ok, latest}
    end
  end

  # Should create new segment?
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
        true
    end
  end

  # Count entries in file
  defp count_entries_in_file(filename) do
    case read_entries_from_file(filename) do
      {:ok, entries} -> {:ok, length(entries)}
      {:error, reason} -> {:error, reason}
    end
  end

  # Read entries from file
  defp read_entries_from_file(filename) do
    case File.read(filename) do
      {:ok, data} -> {:ok, parse_entries(data, [])}
      {:error, reason} -> {:error, reason}
    end
  end

  # Discover segment files
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

  # Get all segments containing log greater than start_index
  defp get_all_segments_containing_log_greater_than_impl(start_index) do
    case discover_segment_files() do
      {:ok, segments} ->
        sorted_segments = Enum.sort_by(segments, fn {_filename, offset} -> offset end, :desc)
        filtered_segments = filter_segments_from_end(sorted_segments, start_index, [])
        {:ok, Enum.reverse(filtered_segments)}

      {:error, reason} ->
        {:error, reason}
    end
  end

  # Filter segments from end
  defp filter_segments_from_end([], _start_index, acc), do: acc

  defp filter_segments_from_end([{filename, base_offset} | rest], start_index, acc) do
    new_acc = [{filename, base_offset} | acc]

    if base_offset <= start_index do
      new_acc
    else
      filter_segments_from_end(rest, start_index, new_acc)
    end
  end

  # Read WAL entries from segments, filtering by start index
  defp read_wal_entries_from(start_index, segments) do
    try do
      all_entries =
        segments
        |> Enum.flat_map(fn {filename, _base_offset} ->
          case read_entries_from_file(filename) do
            {:ok, entries} ->
              Enum.filter(entries, fn entry -> entry.entry_index >= start_index end)

            {:error, _reason} ->
              []
          end
        end)
        |> Enum.sort_by(fn entry -> entry.entry_index end)

      {:ok, all_entries}
    rescue
      error -> {:error, {:read_failed, error}}
    end
  end
end
