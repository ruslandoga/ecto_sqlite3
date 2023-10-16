defmodule Ecto.Adapters.SQLite3.Writer do
  @moduledoc false
  use GenServer

  @typep state :: %{
           conn: Exqlite.conn(),
           queue: :queue.queue({GenServer.from(), reference}),
           writer: reference | nil
         }

  def start_link(opts) do
    GenServer.start_link(__MODULE__, opts)
  end

  @spec insert_all(pid | GenServer.name(), iodata, [[term]], timeout) ::
          :ok | {:error, Exqlite.Error.t()}
  def insert_all(pid, sql, rows, timeout \\ 5000) do
    conn = GenServer.call(pid, :out, timeout)

    try do
      Exqlite.prepare_insert_all(conn, sql, rows)
    after
      GenServer.cast(pid, :in)
    end
  end

  @spec query(pid | GenServer.name(), iodata, [term], timeout) ::
          {:ok, [[term]]} | {:error, Exqlite.Error.t()}
  def query(pid, sql, params, timeout \\ 5000) do
    conn = GenServer.call(pid, :out, timeout)

    try do
      Exqlite.prepare_fetch_all(conn, sql, params)
    after
      GenServer.cast(pid, :in)
    end
  end

  @impl true
  def init(opts) do
    database = Keyword.fetch!(opts, :database)
    flags = Keyword.get(opts, :flags, [:readwrite, :create, :exrescode])

    with {:ok, conn} <- Exqlite.open(database, flags) do
      :ok = Exqlite.execute(conn, "PRAGMA journal_mode=wal")
      :ok = Exqlite.execute(conn, "PRAGMA foreign_keys=on")

      if after_connect = Keyword.get(opts, :after_connect) do
        :ok = after_connect.(conn)
      end

      {:ok, %{conn: conn, queue: :queue.new(), writer: nil}}
    end
  end

  @impl true
  @spec handle_call(:out, GenServer.from(), state) ::
          {:reply, Exqlite.conn(), state} | {:noreply, state}
  def handle_call(:out, from, state) do
    %{conn: conn, writer: writer, queue: queue} = state
    {pid, _} = from
    ref = Process.monitor(pid)

    case writer do
      nil -> {:reply, conn, %{state | writer: ref}}
      _pid -> {:noreply, %{state | queue: :queue.in({from, ref}, queue)}}
    end
  end

  @impl true
  @spec handle_cast(:in, state) :: {:noreply, state}
  def handle_cast(:in, state) do
    case :queue.out(state.queue) do
      {:empty, queue} ->
        {:noreply, %{state | writer: nil, queue: queue}}

      {{:value, {from, ref}}, queue} ->
        :ok = GenServer.reply(from, state.conn)
        {:noreply, %{state | writer: ref, queue: queue}}
    end
  end

  @impl true
  @spec handle_info({:DOWN, reference, :process, pid, any}, state) :: {:noreply, state}
  def handle_info({:DOWN, ref, _, _, _}, %{writer: ref} = state) do
    Process.demonitor(ref, [:flush])

    case :queue.out(state.queue) do
      {:empty, queue} ->
        {:noreply, %{state | writer: nil, queue: queue}}

      {{:value, {from, ref}}, queue} ->
        :ok = GenServer.reply(from, state.conn)
        {:noreply, %{state | writer: ref, queue: queue}}
    end
  end

  def handle_info({:DOWN, ref, _, _, _}, state) do
    Process.demonitor(ref, [:flush])
    queue = :queue.delete_with(fn {_from, qref} -> ref == qref end, state.queue)
    {:noreply, %{state | queue: queue}}
  end
end
