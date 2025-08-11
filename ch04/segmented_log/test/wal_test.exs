defmodule WalTest do
  use ExUnit.Case
  doctest Wal

  test "greets the world" do
    assert Wal.hello() == :world
  end
end
