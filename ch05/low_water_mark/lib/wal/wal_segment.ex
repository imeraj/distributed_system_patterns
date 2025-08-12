defmodule Wal.WALSegment do
  @moduledoc """
  Handles segmented Write-Ahead Log (WAL) filename creation and parsing.

  This module provides utilities for creating consistent filenames for WAL segments
  and extracting base offsets from existing segment filenames.

  ## Filename Format

  WAL segment files follow the pattern: `{prefix}_{start_index}{suffix}`

  For example: `wal_0000000001.log`, `wal_0000001024.log`

  ## Usage

      # Create a filename for a segment starting at index 1024
      filename = Wal.WALSegment.create_filename(1024)
      # Returns "wal_0000001024.log"

      # Parse a filename to get the base offset
      {:ok, offset} = Wal.WALSegment.get_base_offset_from_filename("wal_0000001024.log")
      # Returns {:ok, 1024}
  """

  @log_prefix "wal"
  @log_suffix ".log"
  @index_padding 10

  @type start_index :: non_neg_integer()
  @type filename :: String.t()
  @type base_offset :: non_neg_integer()

  @doc """
  Creates a filename for a WAL segment based on its starting index.

  The filename follows the format: `{prefix}_{padded_start_index}{suffix}`
  where the start index is zero-padded to ensure consistent sorting.

  ## Parameters
  - `start_index`: The starting index of the WAL segment (non-negative integer)

  ## Returns
  - A string representing the segment filename

  ## Examples

      iex> Wal.WALSegment.create_filename(0)
      "wal_0000000000.log"

      iex> Wal.WALSegment.create_filename(1024)
      "wal_0000001024.log"

      iex> Wal.WALSegment.create_filename(999999999)
      "wal_0999999999.log"
  """
  @spec create_filename(start_index()) :: filename()
  def create_filename(start_index) when is_integer(start_index) and start_index >= 0 do
    padded_index = String.pad_leading(Integer.to_string(start_index), @index_padding, "0")
    "#{@log_prefix}_#{padded_index}#{@log_suffix}"
  end

  @doc """
  Extracts the base offset (starting index) from a WAL segment filename.

  Parses a filename that follows the expected WAL segment format and returns
  the starting index of that segment.

  ## Parameters
  - `filename`: The WAL segment filename to parse

  ## Returns
  - `{:ok, base_offset}` if the filename is valid and can be parsed
  - `{:error, :invalid_format}` if the filename doesn't match expected format
  - `{:error, :invalid_prefix}` if the filename doesn't start with expected prefix
  - `{:error, :invalid_index}` if the index portion cannot be parsed as integer

  ## Examples

      iex> Wal.WALSegment.get_base_offset_from_filename("wal_0000001024.log")
      {:ok, 1024}

      iex> Wal.WALSegment.get_base_offset_from_filename("wal_0000000000.log")
      {:ok, 0}

      iex> Wal.WALSegment.get_base_offset_from_filename("invalid_0000001024.log")
      {:error, :invalid_prefix}

      iex> Wal.WALSegment.get_base_offset_from_filename("wal_abc.log")
      {:error, :invalid_index}

      iex> Wal.WALSegment.get_base_offset_from_filename("wal_1024.txt")
      {:error, :invalid_format}
  """
  @spec get_base_offset_from_filename(filename()) ::
          {:ok, base_offset()} | {:error, :invalid_format | :invalid_prefix | :invalid_index}
  def get_base_offset_from_filename(filename) when is_binary(filename) do
    # Check if filename ends with the expected suffix
    unless String.ends_with?(filename, @log_suffix) do
      {:error, :invalid_format}
    else
      # Remove suffix and split on underscore
      name_without_suffix = String.trim_trailing(filename, @log_suffix)

      case String.split(name_without_suffix, "_") do
        [prefix, index_str] when prefix == @log_prefix ->
          case Integer.parse(index_str) do
            {index, ""} when index >= 0 ->
              {:ok, index}

            _ ->
              {:error, :invalid_index}
          end

        _ ->
          {:error, :invalid_format}
      end
    end
  end
end
