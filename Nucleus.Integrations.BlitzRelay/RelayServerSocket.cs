using System;
using System.Collections.Generic;
using System.Net;
using System.Threading.Tasks;
using BlitzRelay.Protocol;
using CodeBoost.Logging;
using CodeBoost.Performance;
using Nucleus.Connections;
using Nucleus.Packets;
using Nucleus.Transports;
using Nucleus.Transports.Sockets;

namespace Nucleus.Integrations.BlitzRelay
{
    /// <summary>
    /// The authority's side of a relayed session. Creates a room on the relay and treats every client the relay admits to it as
    /// an ordinary Nucleus <see cref="Connection"/>.
    /// </summary>
    /// <remarks>
    /// The authority here is not a listening server: it is another of the relay's clients, holding the host role in one room. So
    /// there is no port to bind and no address a peer dials. A peer reaches this one by knowing the room code the relay chose,
    /// which is why <see cref="RelayTransport.RoomCode"/> is the thing worth distributing after a connect.
    /// </remarks>
    public class RelayServerSocket : CommonSocket
    {
        /// <inheritdoc/>
        public override bool IsServer { get; protected set; } = true;

        /// <inheritdoc/>
        public override bool IsEmulated { get; protected set; } = false;

        /// <summary>
        /// The room the relay created for this peer, or empty before it has.
        /// </summary>
        internal string RoomCode => _relayLink?.RoomCode ?? string.Empty;

        /// <summary>
        /// Assigns the per-socket ids remote clients are known by.
        /// </summary>
        /// <remarks>Swept once a frame by the TransportManager through <see cref="SweepIdPools"/>, so it runs no timer of its own.</remarks>
        private readonly DelayedUIntPool _idBySocketPool = new(usesOwnTimer: false);

        /// <summary>
        /// Maps the id the relay addresses a client by onto the Connection the engine knows it as.
        /// </summary>
        private readonly Dictionary<int, Connection> _connectionsByVirtualClientId = new();

        /// <summary>
        /// The reverse, so an outbound packet can be addressed to the right client.
        /// </summary>
        private readonly Dictionary<Connection, int> _virtualClientIdsByConnection = new();

        /// <summary>
        /// Packets decoded during a poll, awaiting the drain that hands them to the engine.
        /// </summary>
        /// <remarks>A plain Queue: the relay link is poll driven, so its callbacks and this drain are the same thread.</remarks>
        private readonly Queue<IncomingPacket> _pendingPackets = new();

        /// <summary>
        /// Clients that joined or left during a poll, applied on the drain so the engine's state changes never run inside a
        /// receive callback.
        /// </summary>
        private readonly Queue<PendingClientEvent> _pendingClientEvents = new();

        /// <summary>
        /// This peer's link to the relay, or <see langword="null"/> before it connects.
        /// </summary>
        private RelayLink? _relayLink;

        /// <summary>
        /// Creates a new instance.
        /// </summary>
        public RelayServerSocket() { }

        /// <inheritdoc/>
        public override void Initialize(Transport transport)
        {
            base.Initialize(transport);
            InitializeConnection();

            _idBySocketPool.OnRent();
            _idBySocketPool.Initialize(returnMinutes: 5, firstRentedValue: Connection.UnsetId + 1);
        }

        /// <inheritdoc/>
        internal override void SweepIdPools() => _idBySocketPool.Sweep();

        /// <inheritdoc/>
        protected override void InitializeConnection()
        {
            Connection = TransportManager.RentInitializedConnection(this);
        }

        /// <summary>
        /// Opens a room on the relay, and reports connected only once the relay has named it.
        /// </summary>
        /// <returns>The result of the state change.</returns>
        /// <remarks>
        /// Waits for the room rather than reporting connected the moment the socket opens, because the room code is the only way
        /// anybody reaches this peer: a caller that had to poll for it afterwards would have nothing to hand out in between.
        /// </remarks>
        internal override async Task<ConnectionStateChangeResult> ConnectAsync()
        {
            RelayTransport relayTransport = (RelayTransport)Transport;

            /* Any previous link is released before another is taken. A peer stands a room up more than once over a session: it
             * declines an election and is elected again, or gives the session up and is later handed it back. Building a second
             * link over a live one would leave the first subscribed and holding its socket for as long as this peer runs. */
            ReleaseRelayLink();

            _relayLink = RelayLink.CreateHost(relayTransport.ConnectionKey, relayTransport.RoomSize);
            _relayLink.ClientJoined += OnRelayClientJoined;
            _relayLink.ClientLeft += OnRelayClientLeft;
            _relayLink.DataReceived += OnRelayDataReceived;

            if (!_relayLink.TryConnect(relayTransport.RelayEndPoint, relayTransport.Configuration.MaximumTransmissionUnit))
                return await FailAsync();

            if (!await WaitUntilReadyAsync(relayTransport.RelayHandshakeTimeoutMilliseconds))
            {
                Logger<RelayServerSocket>.LogError($"The relay at [{relayTransport.RelayEndPoint}] did not create a room within [{relayTransport.RelayHandshakeTimeoutMilliseconds}]ms.");

                return await FailAsync();
            }

            foreach (LocalConnectionState localConnectionState in IterateLocalStateToConnectedOrDisconnected(Invoker.Server, LocalConnectionState.Connected))
            {
                if (localConnectionState is LocalConnectionState.Error)
                    return await FailAsync();
            }

            return ConnectionStateChangeResult.Success;
        }

        /// <inheritdoc/>
        internal override Task<ConnectionStateChangeResult> DisconnectAsync()
        {
            if (_relayLink is null)
                return Task.FromResult(ConnectionStateChangeResult.AlreadyInState);

            /* Every client is dropped before the link goes. The relay tears an ephemeral room down with its host and tells them
             * itself, but the engine side of each one has to be unwound here: a Connection left registered stays an observer of
             * every started system, and its id and Connection are lost to their pools. */
            DisconnectRemoteClients();

            ReleaseRelayLink();

            foreach (LocalConnectionState localConnectionState in IterateLocalStateToConnectedOrDisconnected(Invoker.Server, LocalConnectionState.Disconnected))
            {
                if (localConnectionState is LocalConnectionState.Error)
                    return Task.FromResult(ConnectionStateChangeResult.UnspecifiedError);
            }

            return Task.FromResult(ConnectionStateChangeResult.Success);
        }

        /// <inheritdoc/>
        internal override Task<ConnectionStateChangeResult> DisconnectRemoteClientAsync(Connection connection)
        {
            if (State != LocalConnectionState.Connected || _relayLink is null)
                return Task.FromResult(ConnectionStateChangeResult.InvalidSocketState);

            if (connection is null || !_virtualClientIdsByConnection.TryGetValue(connection, out int virtualClientId))
                return Task.FromResult(ConnectionStateChangeResult.InvalidConnection);

            _relayLink.KickClient(virtualClientId);

            return Task.FromResult(ConnectionStateChangeResult.Success);
        }

        /// <inheritdoc/>
        internal override void SendPacket(OutgoingPacket outgoingPacket)
        {
            if (_relayLink is null || State != LocalConnectionState.Connected)
            {
                Managers.Transports.TransportManager.PacketFailed(outgoingPacket);

                return;
            }

            if (outgoingPacket.Receiver is null || !_virtualClientIdsByConnection.TryGetValue(outgoingPacket.Receiver, out int virtualClientId))
            {
                Logger<RelayServerSocket>.LogError($"No relay client is mapped to receiver [{outgoingPacket.Receiver.AsString()}].");
                Managers.Transports.TransportManager.PacketFailed(outgoingPacket);

                return;
            }

            if (!_relayLink.TrySendAsHost(virtualClientId, outgoingPacket.Channel, outgoingPacket.Payload))
            {
                Managers.Transports.TransportManager.PacketFailed(outgoingPacket);

                return;
            }

            Managers.Transports.TransportManager.PacketOffloaded(outgoingPacket);
        }

        /// <inheritdoc/>
        internal override void ReceivePackets(ref List<IncomingPacket> receivedPackets)
        {
            _relayLink?.Poll();

            while (_pendingClientEvents.TryDequeue(out PendingClientEvent pendingClientEvent))
            {
                if (pendingClientEvent.HasJoined)
                    NotifyConnected(pendingClientEvent.Connection, pendingClientEvent.IdBySocket);
                else
                    TransportManager.SetRemoteConnectionStateAsDisconnected(pendingClientEvent.Connection);
            }

            while (_pendingPackets.TryDequeue(out IncomingPacket incomingPacket))
                receivedPackets.Add(incomingPacket);
        }

        /// <inheritdoc/>
        public override void OnReturn()
        {
            _idBySocketPool.OnReturn();

            /* Held rather than owned: a remote client's Connection goes back to its pool through the TransportManager, so these
             * are cleared and not drained. */
            _connectionsByVirtualClientId.Clear();
            _virtualClientIdsByConnection.Clear();

            _pendingPackets.Clear();
            _pendingClientEvents.Clear();

            base.OnReturn();
        }

        /// <summary>
        /// Unwinds a failed connect, leaving the socket as it was before it was tried.
        /// </summary>
        /// <returns>An unspecified failure, for the caller to return.</returns>
        private async Task<ConnectionStateChangeResult> FailAsync()
        {
            ReleaseRelayLink();

            TransportManager.ChangeLocalConnectionState(Invoker.Server, LocalConnectionState.Disconnected, Connection);

            return await Task.FromResult(ConnectionStateChangeResult.UnspecifiedError);
        }

        /// <summary>
        /// Unsubscribes from this peer's link to the relay and disposes it, leaving nothing to be reached from it.
        /// </summary>
        /// <remarks>
        /// Unsubscribing before disposing matters as much as the disposal does: a link left subscribed can still report a client
        /// arriving or leaving onto a socket that has moved on to another room.
        /// </remarks>
        private void ReleaseRelayLink()
        {
            if (_relayLink is null)
                return;

            _relayLink.ClientJoined -= OnRelayClientJoined;
            _relayLink.ClientLeft -= OnRelayClientLeft;
            _relayLink.DataReceived -= OnRelayDataReceived;

            _relayLink.Dispose();
            _relayLink = null;
        }

        /// <summary>
        /// Drives the link until the relay has answered the handshake, or the wait runs out.
        /// </summary>
        /// <param name="timeoutMilliseconds">How long to wait.</param>
        /// <returns><see langword="true"/> when the relay answered.</returns>
        /// <remarks>
        /// Polls the link from here rather than waiting for the network loop, because the loop does not drive a socket that has
        /// not reported itself connected, and this is what decides whether it can.
        /// </remarks>
        private async Task<bool> WaitUntilReadyAsync(uint timeoutMilliseconds)
        {
            const int PollIntervalMilliseconds = 5;

            for (uint elapsedMilliseconds = 0; elapsedMilliseconds < timeoutMilliseconds; elapsedMilliseconds += PollIntervalMilliseconds)
            {
                _relayLink?.Poll();

                if (_relayLink is not null && _relayLink.IsReady)
                    return true;

                await Task.Delay(PollIntervalMilliseconds);
            }

            return _relayLink is not null && _relayLink.IsReady;
        }

        /// <summary>
        /// Drops every client this peer is serving, and unwinds each one's engine-side state.
        /// </summary>
        /// <remarks>
        /// Runs on the caller's thread rather than through the pending queue, because a stopping socket is drained no further and
        /// anything queued here would never be applied.
        /// </remarks>
        private void DisconnectRemoteClients()
        {
            if (_connectionsByVirtualClientId.Count == 0)
                return;

            List<Connection> departingConnections = ListPool<Connection>.Rent();

            foreach (KeyValuePair<int, Connection> connectionByVirtualClientId in _connectionsByVirtualClientId)
                departingConnections.Add(connectionByVirtualClientId.Value);

            _connectionsByVirtualClientId.Clear();
            _virtualClientIdsByConnection.Clear();

            foreach (Connection departingConnection in departingConnections)
                TransportManager.SetRemoteConnectionStateAsDisconnected(departingConnection);

            ListPool<Connection>.ReturnAndNullifyReference(ref departingConnections);
        }

        /// <summary>
        /// Adopts a client the relay has admitted to this peer's room.
        /// </summary>
        /// <param name="virtualClientId">The id the relay addresses it by.</param>
        private void OnRelayClientJoined(int virtualClientId)
        {
            if (_connectionsByVirtualClientId.ContainsKey(virtualClientId))
                return;

            if (!_idBySocketPool.EnsureRent(out uint idBySocket))
            {
                Logger<RelayServerSocket>.LogError($"No id could be rented for relay client [{virtualClientId}]. Dropping it.");

                _relayLink?.KickClient(virtualClientId);

                return;
            }

            /* The relay's own id for the peer, and nothing else. A relayed peer has no address this side can observe: every
             * datagram arrives from the relay, so an address-based authenticator has nothing to work with here. */
            Connection connection = ResettableObjectPool<Connection>.Rent();
            connection.Initialize(idBySocket, isEmulated: false, Transport, remoteAddress: virtualClientId.ToString());

            _connectionsByVirtualClientId[virtualClientId] = connection;
            _virtualClientIdsByConnection[connection] = virtualClientId;

            _pendingClientEvents.Enqueue(new PendingClientEvent(connection, idBySocket, hasJoined: true));
        }

        /// <summary>
        /// Releases a client that has left this peer's room.
        /// </summary>
        /// <param name="virtualClientId">The id the relay addressed it by.</param>
        private void OnRelayClientLeft(int virtualClientId)
        {
            if (!_connectionsByVirtualClientId.TryGetValue(virtualClientId, out Connection connection))
                return;

            _connectionsByVirtualClientId.Remove(virtualClientId);
            _virtualClientIdsByConnection.Remove(connection);

            _pendingClientEvents.Enqueue(new PendingClientEvent(connection, connection.IdBySocket, hasJoined: false));
        }

        /// <summary>
        /// Queues game data from one of this peer's clients.
        /// </summary>
        /// <param name="virtualClientId">The client it came from.</param>
        /// <param name="channel">The channel it rode.</param>
        /// <param name="payload">The data.</param>
        private void OnRelayDataReceived(int virtualClientId, Channel channel, ArraySegment<byte> payload)
        {
            if (!_connectionsByVirtualClientId.TryGetValue(virtualClientId, out Connection senderConnection))
            {
                Logger<RelayServerSocket>.LogError($"Data arrived from relay client [{virtualClientId}] before it joined; dropping it.");

                return;
            }

            _pendingPackets.Enqueue(new IncomingPacket(payload, channel, senderConnection));
        }

        /// <summary>
        /// Registers a joined client with the engine.
        /// </summary>
        /// <param name="connection">The client's Connection.</param>
        /// <param name="idBySocket">The id it was rented.</param>
        private void NotifyConnected(Connection connection, uint idBySocket)
        {
            ConnectionStateChangeResult connectionStateChangeResult = TransportManager.SetRemoteConnectionStateAsConnected(idBySocket, connection);

            if (connectionStateChangeResult is ConnectionStateChangeResult.Success)
                return;

            Logger<RelayServerSocket>.LogError($"A joined relay client could not be registered: [{connectionStateChangeResult}].");

            if (_virtualClientIdsByConnection.TryGetValue(connection, out int virtualClientId))
            {
                _connectionsByVirtualClientId.Remove(virtualClientId);
                _virtualClientIdsByConnection.Remove(connection);
            }

            _idBySocketPool.ReturnUnsafe(idBySocket);
            ResettableObjectPool<Connection>.Return(connection);
        }

        /// <summary>
        /// A client joining or leaving during a poll, applied once the drain reaches it.
        /// </summary>
        private readonly struct PendingClientEvent
        {
            /// <summary>
            /// The client's Connection.
            /// </summary>
            public readonly Connection Connection;

            /// <summary>
            /// The id it was rented.
            /// </summary>
            public readonly uint IdBySocket;

            /// <summary>
            /// Whether it joined, as against left.
            /// </summary>
            public readonly bool HasJoined;

            /// <summary>
            /// Creates a new instance.
            /// </summary>
            /// <param name="connection">The client's Connection.</param>
            /// <param name="idBySocket">The id it was rented.</param>
            /// <param name="hasJoined">Whether it joined.</param>
            public PendingClientEvent(Connection connection, uint idBySocket, bool hasJoined)
            {
                Connection = connection;
                IdBySocket = idBySocket;
                HasJoined = hasJoined;
            }
        }
    }
}
