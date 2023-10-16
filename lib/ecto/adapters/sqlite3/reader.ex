defmodule Ecto.Adapters.SQLite3.Reader do
  @moduledoc false
  use GenServer

  @typep state :: %{
           conn: Exqlite.conn(),
           data_version_conn: Exqlite.conn(),
           data_version_stmt: Exqlite.stmt(),
           queue: nil | [{GenServer.from(), reference()}]
         }

  def start_link(config) do
    GenServer.start_link(__MODULE__, config)
  end

  @spec fetch_all(GenServer.name() | pid, iodata, [term], timeout) ::
          {:ok, [[term]]} | {:error, Exqlite.Error.t()}
  def fetch_all(pid, sql, params, timeout \\ 5000) do
    conn = GenServer.call(pid, :out, timeout)

    try do
      Exqlite.prepare_fetch_all(conn, sql, params)
    after
      GenServer.cast(pid, :in)
    end
  end

  @impl true
  @spec init(Keyword.t()) :: {:ok, state} | {:error, Exqlite.Error.t()}
  def init(config) do
    path = Keyword.fetch!(config, :database)
    flags = Keyword.get(config, :flags, [:readonly, :exrescode])
    readonly_flags = [:readonly | List.delete(flags, :readwrite)]

    with {:ok, conn} <- Exqlite.open(path, readonly_flags) do
      {:ok, data_version_conn} = Exqlite.open(path, readonly_flags)

      {:ok, data_version_stmt} =
        Exqlite.prepare(data_version_conn, "pragma data_version")

      {:ok, [[data_version]]} =
        Exqlite.fetch_all(data_version_conn, data_version_stmt)

      if after_connect = Keyword.get(config, :after_connect) do
        # TODO expose SQLite.readonly?(db)
        :ok = after_connect.(conn)
      end

      {:ok,
       %{
         conn: conn,
         data_version: data_version,
         data_version_conn: data_version_conn,
         data_version_stmt: data_version_stmt,
         queue: nil
       }}
    end
  end

  @impl true
  @spec handle_call(:out, GenServer.from(), state) ::
          {:reply, Exqlite.conn(), state} | {:noreply, state}
  def handle_call(:out, from, state) do
    %{
      conn: conn,
      data_version: data_version,
      data_version_conn: data_version_conn,
      data_version_stmt: data_version_stmt,
      queue: queue
    } = state

    IO.inspect(state, label: "state")

    if queue do
      {pid, _} = from
      ref = Process.monitor(pid)
      {:noreply, %{state | queue: [{from, ref} | queue]}}
    else
      {:ok, [[current_data_version]]} =
        Exqlite.fetch_all(data_version_conn, data_version_stmt)

      if data_version == current_data_version do
        {:reply, conn, state}
      else
        {pid, _} = from
        ref = Process.monitor(pid)
        {:noreply, %{state | data_version: data_version, queue: [{from, ref}]}}
      end
    end
  end

  @impl true
  @spec handle_cast(:in, state) :: {:noreply, state}
  def handle_cast(:in, state) do
    %{
      conn: conn,
      data_version_conn: data_version_conn,
      data_version_stmt: data_version_stmt,
      queue: queue
    } = state

    {:ok, tx_status} = Exqlite.transaction_status(conn)

    if queue && tx_status == :idle do
      Enum.each(queue, fn {from, ref} ->
        Process.demonitor(ref, [:flush])
        GenServer.reply(from, conn)
      end)

      {:ok, [[data_version]]} =
        Exqlite.fetch_all(data_version_conn, data_version_stmt)

      {:noreply, %{state | data_version: data_version, queue: nil}}
    else
      {:noreply, state}
    end
  end

  @impl true
  @spec handle_info({:DOWN, reference, :process, pid, any}, state) :: {:noreply, state}
  def handle_info({:DOWN, _, _, _, _}, state) do
    %{
      conn: conn,
      data_version_conn: data_version_conn,
      data_version_stmt: data_version_stmt,
      queue: queue
    } = state

    {:ok, tx_status} = Exqlite.transaction_status(conn)

    if queue && tx_status == :idle do
      Enum.each(queue, fn {from, ref} ->
        Process.demonitor(ref, [:flush])
        GenServer.reply(from, conn)
      end)

      {:ok, [[data_version]]} =
        Exqlite.fetch_all(data_version_conn, data_version_stmt)

      {:noreply, %{state | data_version: data_version, queue: nil}}
    else
      {:noreply, state}
    end
  end
end
