using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using CodeBoost.Logging;
using Nucleus.Connections;
using Nucleus.Packets;
using Nucleus.Transports;
using Nucleus.Transports.Sockets;

namespace Nucleus.Integrations.BlitzRelay
{
    /// <summary>
    /// A joining peer's side of a relayed session. Joins the room named by <see cref="RelayTransport.RoomCode"/> and treats the
    /// peer hosting it as the authority.
    /// </summary>
    /// <remarks>
    /// There is no address to dial here. A room code is the whole of what this peer needs, and it reaches the authority only
    /// because the relay is willing to carry it.
    /// </remarks>
    public class RelayClientSocket : CommonSocket
    {
        /// <inheritdoc/>
        public override bool IsServer { get; protected set; } = false;

        /// <inheritdoc/>
        public override bool IsEmulated { get; protected set; } = false;

        /// <summary>
        /// Packets decoded during a poll, awaiting the drain that hands them to the engine.
        /// </summary>
        private readonly Queue<IncomingPacket> _pendingPackets = new();

        /// <summary>
        /// This peer's link to the relay, or <see langword="null"/> before it connects.
        /// </summary>
        private RelayLink? _relayLink;

        /// <summary>
        /// Stands in as the sender on inbound packets.
        /// </summary>
        /// <remarks>
        /// The engine attributes an inbound client packet through the transport's server-side Connection rather than through the
        /// sender on the packet, so this is only ever read to reach the Transport. The server socket's own Connection on this same
        /// transport is the right stand-in, and its reference is fixed for the transport's life.
        /// </remarks>
        private Connection? _authorityStandIn;

        /// <summary>
        /// True when the relay has closed this peer's link and the engine has yet to be told.
        /// </summary>
        private bool _isRelayLinkClosed;

        /// <summary>
        /// Creates a new instance.
        /// </summary>
        public RelayClientSocket() { }

        /// <inheritdoc/>
        public override void Initialize(Transport transport)
        {
            base.Initialize(transport);
            InitializeConnection();
        }

        /// <inheritdoc/>
        protected override void InitializeConnection()
        {
            Connection = TransportManager.RentInitializedConnection(this);
        }

        /// <summary>
        /// Joins the configured room, and reports connected only once the relay has let this peer in.
        /// </summary>
        /// <returns>The result of the state change.</returns>
        /// <remarks>
        /// Waiting matters more here than on the authority's side: a peer the relay has not yet admitted to a room is a peer
        /// whose datagrams the relay discards, so reporting connected early would lose the handshake the engine sends first.
        /// </remarks>
        internal override async Task<ConnectionStateChangeResult> ConnectAsync()
        {
            RelayTransport relayTransport = (RelayTransport)Transport;

            if (string.IsNullOrEmpty(relayTransport.RoomCode))
            {
                Logger<RelayClientSocket>.LogError($"No room to join. Set [{nameof(RelayTransport)}.{nameof(RelayTransport.RoomCode)}] to the room the authority is hosting before connecting.");

                return ConnectionStateChangeResult.UnspecifiedError;
            }

            _authorityStandIn = ((RelayTransport)Transport).AuthorityStandIn;
            _isRelayLinkClosed = false;

            /* Captured once, because the failure log below can run long after the transport's RoomCode has legitimately moved on:
             * a coordinator that gave up on this room and was promoted into another would otherwise be reported as failing to
             * join the room it is hosting. */
            string joiningRoomCode = relayTransport.RoomCode;

            _relayLink = RelayLink.CreateClient(relayTransport.ConnectionKey, joiningRoomCode);
            _relayLink.DataReceived += OnRelayDataReceived;
            _relayLink.Closed += OnRelayLinkClosed;

            if (!_relayLink.TryConnect(relayTransport.RelayEndPoint, relayTransport.Configuration.MaximumTransmissionUnit))
                return await FailAsync();

            if (!await WaitUntilReadyAsync(relayTransport.RelayHandshakeTimeoutMilliseconds))
            {
                Logger<RelayClientSocket>.LogError($"The relay at [{relayTransport.RelayEndPoint}] did not admit this peer to room [{joiningRoomCode}] within [{relayTransport.RelayHandshakeTimeoutMilliseconds}]ms, or refused it.");

                return await FailAsync();
            }

            foreach (LocalConnectionState localConnectionState in IterateLocalStateToConnectedOrDisconnected(Invoker.Client, LocalConnectionState.Connected))
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

            _relayLink.DataReceived -= OnRelayDataReceived;
            _relayLink.Closed -= OnRelayLinkClosed;

            _relayLink.Dispose();
            _relayLink = null;

            foreach (LocalConnectionState localConnectionState in IterateLocalStateToConnectedOrDisconnected(Invoker.Client, LocalConnectionState.Disconnected))
            {
                if (localConnectionState is LocalConnectionState.Error)
                    return Task.FromResult(ConnectionStateChangeResult.UnspecifiedError);
            }

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

            if (!_relayLink.TrySendAsClient(outgoingPacket.Channel, outgoingPacket.Payload))
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

            // Applied here, on the loop thread, so the state change and its callbacks never run inside a receive callback.
            if (_isRelayLinkClosed)
            {
                _isRelayLinkClosed = false;

                TransportManager.ChangeLocalConnectionState(Invoker.Client, LocalConnectionState.Disconnected, Connection);
            }

            while (_pendingPackets.TryDequeue(out IncomingPacket incomingPacket))
                receivedPackets.Add(incomingPacket);
        }

        /// <inheritdoc/>
        public override void OnReturn()
        {
            _pendingPackets.Clear();

            _authorityStandIn = null;

            base.OnReturn();
        }

        /// <summary>
        /// Unwinds a failed connect, leaving the socket as it was before it was tried.
        /// </summary>
        /// <returns>An unspecified failure, for the caller to return.</returns>
        private async Task<ConnectionStateChangeResult> FailAsync()
        {
            _relayLink?.Dispose();
            _relayLink = null;

            TransportManager.ChangeLocalConnectionState(Invoker.Client, LocalConnectionState.Disconnected, Connection);

            return await Task.FromResult(ConnectionStateChangeResult.UnspecifiedError);
        }

        /// <summary>
        /// Drives the link until the relay has admitted this peer, refused it, or the wait runs out.
        /// </summary>
        /// <param name="timeoutMilliseconds">How long to wait.</param>
        /// <returns><see langword="true"/> when the relay admitted it.</returns>
        private async Task<bool> WaitUntilReadyAsync(uint timeoutMilliseconds)
        {
            const int PollIntervalMilliseconds = 5;

            for (uint elapsedMilliseconds = 0; elapsedMilliseconds < timeoutMilliseconds; elapsedMilliseconds += PollIntervalMilliseconds)
            {
                _relayLink?.Poll();

                if (_relayLink is not null && _relayLink.IsReady)
                    return true;

                /* The relay saying no is an answer, not a slow yes: a refusal closes the link, and a join asked of a room that
                 * died with its host is refused in one round trip. Waiting out the handshake window anyway held a returning
                 * peer's whole bootstrap for it. The flag is left standing for the receive pass, which is its consumer. */
                if (_isRelayLinkClosed)
                    return false;

                await Task.Delay(PollIntervalMilliseconds);
            }

            return _relayLink is not null && _relayLink.IsReady;
        }

        /// <summary>
        /// Queues game data from the authority.
        /// </summary>
        /// <param name="virtualClientId">Unused: a client only ever hears from the peer hosting it.</param>
        /// <param name="channel">The channel it rode.</param>
        /// <param name="payload">The data.</param>
        private void OnRelayDataReceived(int virtualClientId, Channel channel, ArraySegment<byte> payload)
        {
            if (_authorityStandIn is null)
            {
                Logger<RelayClientSocket>.LogError("Data arrived before this socket connected; dropping it.");

                return;
            }

            _pendingPackets.Enqueue(new IncomingPacket(payload, channel, _authorityStandIn));
        }

        /// <summary>
        /// Records that the relay has closed this peer's link, which is what the room being torn down looks like from here.
        /// </summary>
        private void OnRelayLinkClosed()
        {
            _isRelayLinkClosed = true;
        }
    }
}
