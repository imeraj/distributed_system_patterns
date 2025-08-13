defmodule Wal.KVStore do
  use GenServer

  @moduledoc """
  A key-value store GenServer that persists operations to a Write-Ahead Log (WAL).

  This GenServer maintains an in-memory map of key-value pairs and logs all
  write operations to a WAL file for durability and crash recovery.

  ## Usage

      {:ok, pid} = Wal.KVStore.start_link()

      :ok = Wal.KVStore.put(pid, "user:123", "John Doe")
      {:ok, "John Doe"} = Wal.KVStore.get(pid, "user:123")
      {:error, :not_found} = Wal.KVStore.get(pid, "nonexistent")

  ## State

  The GenServer maintains the following state:
  - `data`: A map containing the key-value pairs
  - `next_index`: Next WAL entry index to use

  ## WAL Format

  Each PUT operation is logged to the WAL as:
  - Entry type: `:set`
  - Data: Serialized `{:set, key, value}` tuple
  - Index: Monotonically increasing integer
  """

  alias Wal.WAL

  # Client API

  @doc """
  Starts the KVStore GenServer.

  ## Options
  - `:name` - Name to register the GenServer (optional)
  - `:wal` - WAL GenServer pid or name (required)

  ## Examples

      {:ok, wal_pid} = Wal.WAL.start_link([])
      {:ok, pid} = Wal.KVStore.start_link(wal: wal_pid)
      {:ok, pid} = Wal.KVStore.start_link(name: :my_store, wal: wal_pid)

  """
  def start_link(opts \\ []) do
    name = Keyword.get(opts, :name, __MODULE__)
    wal = Keyword.fetch!(opts, :wal)
    GenServer.start_link(__MODULE__, %{wal: wal}, name: name)
  end

  @doc """
  Retrieves a value by key.

  ## Parameters
  - `server`: The GenServer pid or registered name
  - `key`: The key to look up

  ## Returns
  - `{:ok, value}` if key exists
  - `{:error, :not_found}` if key doesn't exist

  ## Examples

      {:ok, "John Doe"} = Wal.KVStore.get(pid, "user:123")
      {:error, :not_found} = Wal.KVStore.get(pid, "missing_key")

  """
  def get(server, key) do
    GenServer.call(server, {:get, key})
  end

  @doc """
  Stores a key-value pair.

  This operation is first logged to the WAL before updating the in-memory state.

  ## Parameters
  - `server`: The GenServer pid or registered name
  - `key`: The key to store
  - `value`: The value to store

  ## Returns
  - `:ok` on success
  - `{:error, reason}` on failure

  ## Examples

      :ok = Wal.KVStore.put(pid, "user:123", "John Doe")
      :ok = Wal.KVStore.put(pid, "config:timeout", "30000")

  """
  def put(server, key, value) do
    GenServer.call(server, {:put, key, value})
  end

  @doc """
  Stores multiple key-value pairs in a batch operation.

  This operation writes all entries to the WAL in a single batch before
  updating the in-memory state, providing better performance and atomicity
  for multiple operations.

  ## Parameters
  - `server`: The GenServer pid or registered name
  - `key_value_pairs`: List of {key, value} tuples to store

  ## Returns
  - `:ok` on success
  - `{:error, reason}` on failure

  ## Examples

      pairs = [{"user:123", "John"}, {"user:124", "Jane"}]
      :ok = Wal.KVStore.put_batch(pid, pairs)

  """
  def put_batch(server, key_value_pairs) do
    GenServer.call(server, {:put_batch, key_value_pairs})
  end

  @doc """
  Returns the current state of the KVStore for debugging/inspection.

  ## Parameters
  - `server`: The GenServer pid or registered name

  ## Returns
  - `%{data: map(), next_index: integer()}`

  ## Examples

      state = Wal.KVStore.get_state(pid)
      IO.inspect(state.data)

  """
  def get_state(server) do
    GenServer.call(server, :get_state)
  end

  @doc """
  Recovers the KVStore state from a specific WAL index.

  This can be useful for partial recovery scenarios or when you want to
  rebuild state from a particular point in the WAL history.

  ## Parameters
  - `server`: The GenServer pid or registered name
  - `start_index`: The starting index to recover from

  ## Returns
  - `:ok` on success
  - `{:error, reason}` on failure
  """
  def recover_from_index(server, start_index) do
    GenServer.call(server, {:recover_from_index, start_index})
  end

  @doc """
  Gets information about WAL segments currently in use.

  Returns statistics about the segmented WAL including number of segments,
  total entries, and current active segment.

  ## Parameters
  - `server`: The GenServer pid or registered name

  ## Returns
  - `{:ok, segment_info}` on success
  - `{:error, reason}` on failure

  ## Examples

      {:ok, info} = Wal.KVStore.get_segment_info(pid)
      IO.inspect(info.segment_count)
  """
  def get_segment_info(server) do
    GenServer.call(server, :get_segment_info)
  end

  @doc """
  Gets the current active segment filename being written to.

  ## Parameters
  - `server`: The GenServer pid or registered name

  ## Returns
  - `{:ok, segment_filename}` on success
  - `{:error, reason}` on failure
  """
  def get_current_segment(server) do
    GenServer.call(server, :get_current_segment)
  end

  # GenServer callbacks

  @impl true
  def init(%{wal: wal} = _opts) do
    # Recover state from WAL GenServer if it exists
    {data, next_index} = recover_from_wal(wal)

    state = %{
      data: data,
      next_index: next_index,
      wal: wal
    }

    {:ok, state}
  end

  @impl true
  def handle_call({:get, key}, _from, state) do
    case Map.get(state.data, key) do
      nil -> {:reply, {:error, :not_found}, state}
      value -> {:reply, {:ok, value}, state}
    end
  end

  @impl true
  def handle_call({:put, key, value}, _from, state) do
    # Create WAL entry
    wal_entry =
      WAL.create_entry(state.next_index, :erlang.term_to_binary({:set, key, value}), :set)

    case WAL.write_entry(state.wal, wal_entry) do
      :ok ->
        # Update in-memory state only after successful WAL write
        new_data = Map.put(state.data, key, value)
        new_next_index = state.next_index + 1
        WAL.set_last_index(state.wal, new_next_index - 1)
        new_state = %{state | data: new_data, next_index: new_next_index}
        {:reply, :ok, new_state}

      {:error, reason} ->
        {:reply, {:error, reason}, state}
    end
  end

  @impl true
  def handle_call({:put_batch, key_value_pairs}, _from, state) do
    # Create WAL entries for all key-value pairs
    wal_entries =
      key_value_pairs
      |> Enum.with_index()
      |> Enum.map(fn {{key, value}, index} ->
        entry_index = state.next_index + index
        WAL.create_entry(entry_index, :erlang.term_to_binary({:set, key, value}), :set)
      end)

    case WAL.write_entries(state.wal, wal_entries) do
      :ok ->
        # Update in-memory state only after successful WAL write
        new_data =
          Enum.reduce(key_value_pairs, state.data, fn {key, value}, acc ->
            Map.put(acc, key, value)
          end)

        new_next_index = state.next_index + length(key_value_pairs)
        WAL.set_last_index(state.wal, new_next_index - 1)
        new_state = %{state | data: new_data, next_index: new_next_index}
        {:reply, :ok, new_state}

      {:error, reason} ->
        {:reply, {:error, reason}, state}
    end
  end

  @impl true
  def handle_call(:get_state, _from, state) do
    {:reply, state, state}
  end

  @impl true
  def handle_call({:recover_from_index, start_index}, _from, state) do
    # Recover state from specified index
    {data, next_index} = recover_from_wal(state.wal, start_index)

    new_state = %{
      data: data,
      next_index: next_index,
      wal: state.wal
    }

    {:reply, :ok, new_state}
  end

  @impl true
  def handle_call(:get_segment_info, _from, state) do
    case WAL.get_all_segments_containing_log_greater_than(state.wal, 0) do
      {:ok, segments} ->
        segment_info = %{
          segment_count: length(segments),
          segments: segments,
          next_index: state.next_index,
          data_entries: map_size(state.data)
        }

        {:reply, {:ok, segment_info}, state}

      {:error, reason} ->
        {:reply, {:error, reason}, state}
    end
  end

  @impl true
  def handle_call(:get_current_segment, _from, state) do
    case WAL.get_current_segment_filename(state.wal, state.next_index) do
      {:ok, segment_filename} ->
        {:reply, {:ok, segment_filename}, state}

      {:error, reason} ->
        {:reply, {:error, reason}, state}
    end
  end

  # Private functions

  defp recover_from_wal(wal, start_index \\ 0) do
    case WAL.read_from(wal, start_index) do
      {:ok, entries} ->
        # Replay entries to rebuild state
        data = replay_entries(entries, %{})

        # Calculate next index based on the highest entry index found
        next_index =
          case entries do
            [] ->
              start_index + 1

            _ ->
              max_index = Enum.max_by(entries, fn entry -> entry.entry_index end).entry_index
              max_index + 1
          end

        # Set last_index in WAL after reading
        WAL.set_last_index(wal, next_index - 1)

        {data, next_index}

      {:error, :enoent} ->
        # No WAL segments exist, start fresh
        {%{}, start_index + 1}

      {:error, _reason} ->
        # WAL segments exist but can't be read, start fresh
        # In production, you might want to handle this differently
        {%{}, start_index + 1}
    end
  end

  defp replay_entries([], data), do: data

  defp replay_entries([entry | rest], data) do
    new_data =
      try do
        case :erlang.binary_to_term(entry.data) do
          {:set, key, value} -> Map.put(data, key, value)
          # Skip invalid entries
          _ -> data
        end
      rescue
        # Skip corrupted entries
        _ -> data
      end

    replay_entries(rest, new_data)
  end
end
