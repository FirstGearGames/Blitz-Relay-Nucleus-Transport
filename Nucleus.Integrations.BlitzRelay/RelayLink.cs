using System;
using System.Buffers;
using System.Collections.Generic;
using System.Net;
using BlitzRelay.Protocol;
using CodeBoost.Logging;
using Nucleus.Transports;
using SynapseManager = SynapseSocket.Core.SynapseManager;
using SynapseConfig = SynapseSocket.Core.Configuration.SynapseConfig;
using SynapseConnection = SynapseSocket.Connections.SynapseConnection;
using ConnectionEventArgs = SynapseSocket.Core.Events.ConnectionEventArgs;
using PacketReceivedEventArgs = SynapseSocket.Core.Events.PacketReceivedEventArgs;

namespace Nucleus.Integrations.BlitzRelay
{
    /// <summary>
    /// One peer's link to a Blitz Relay server: the socket, the handshake, and the relay's framing.
    /// </summary>
    /// <remarks>
    /// Shared by both sides of the transport because both are, to the relay, ordinary clients of it. What differs is the role
    /// asked for after authenticating, and that is the only thing either side has to know about the other.
    /// Poll driven, like the engine it feeds. Nothing here owns a thread, and every event is raised inside
    /// <see cref="Poll"/> on whichever thread called it.
    /// </remarks>
    internal sealed class RelayLink : IDisposable
    {
        /// <summary>
        /// Raised once the relay has answered the handshake and the peer holds the role it asked for.
        /// </summary>
        public event Action? Ready;

        /// <summary>
        /// Raised when the link to the relay is gone, whether the relay closed it or it timed out.
        /// </summary>
        public event Action? Closed;

        /// <summary>
        /// Raised when a client joins the room this peer is hosting, carrying the id the relay addresses it by.
        /// </summary>
        public event Action<int>? ClientJoined;

        /// <summary>
        /// Raised when a client leaves the room this peer is hosting.
        /// </summary>
        public event Action<int>? ClientLeft;

        /// <summary>
        /// Raised for game data, carrying the peer it came from (<see cref="HostVirtualClientId"/> on a client, which only ever
        /// hears from the host), the channel it rode, and the payload.
        /// </summary>
        public event Action<int, Channel, ArraySegment<byte>>? DataReceived;

        /// <summary>
        /// The room this peer is hosting or has joined, or empty until the relay names one.
        /// </summary>
        public string RoomCode { get; private set; } = string.Empty;

        /// <summary>
        /// True once the relay has answered the handshake.
        /// </summary>
        public bool IsReady { get; private set; }

        /// <summary>
        /// The id a client uses to mean "the host", since a client never addresses anybody else. The same value the relay reads
        /// as a broadcast when a host sends it, both meaning "not one particular client".
        /// </summary>
        public const int HostVirtualClientId = -1;

        /// <summary>
        /// What this peer asked the relay to be.
        /// </summary>
        private readonly bool _isHost;

        /// <summary>
        /// The key the relay admits peers with.
        /// </summary>
        private readonly string _connectionKey;

        /// <summary>
        /// How many clients a room this peer creates will hold.
        /// </summary>
        private readonly int _maximumClients;

        /// <summary>
        /// The room a joining peer asks for, unused by a host.
        /// </summary>
        private readonly string _requestedRoomCode;

        /// <summary>
        /// The engine carrying this peer's datagrams to the relay.
        /// </summary>
        private SynapseManager? _synapseManager;

        /// <summary>
        /// The link to the relay, or <see langword="null"/> before it is established.
        /// </summary>
        private SynapseConnection? _relayConnection;

        /// <summary>
        /// True once the handshake has been sent, so it is not sent twice for one link.
        /// </summary>
        private bool _isHandshakeSent;

        /// <summary>
        /// Every outbound message is framed here, so sending allocates nothing.
        /// </summary>
        /// <remarks>
        /// Sized at connect, when the transmission unit is known. One buffer is enough because a send is synchronous and the
        /// socket has copied what it was given by the time it returns.
        /// </remarks>
        private byte[] _sendBuffer = EmptySendBuffer;

        /// <summary>
        /// The widest framing the relay puts around a payload, being a host data frame's type, target and channel.
        /// </summary>
        private const int MaximumFrameHeaderSize = 6;

        /// <summary>
        /// Stands in before a link is connected, so nothing has to be null-checked on a path that never runs then.
        /// </summary>
        private static readonly byte[] EmptySendBuffer = new byte[0];

        /// <summary>
        /// Creates a link that will host a room of its own.
        /// </summary>
        /// <param name="connectionKey">The key the relay admits peers with.</param>
        /// <param name="maximumClients">How many clients the room will hold.</param>
        /// <returns>The link, not yet connected.</returns>
        public static RelayLink CreateHost(string connectionKey, int maximumClients)
        {
            return new RelayLink(isHost: true, connectionKey, maximumClients, requestedRoomCode: string.Empty);
        }

        /// <summary>
        /// Creates a link that will join somebody else's room.
        /// </summary>
        /// <param name="connectionKey">The key the relay admits peers with.</param>
        /// <param name="roomCode">The room to join.</param>
        /// <returns>The link, not yet connected.</returns>
        public static RelayLink CreateClient(string connectionKey, string roomCode)
        {
            return new RelayLink(isHost: false, connectionKey, maximumClients: 0, requestedRoomCode: roomCode);
        }

        /// <summary>
        /// Creates a link.
        /// </summary>
        /// <param name="isHost">Whether this peer will host a room.</param>
        /// <param name="connectionKey">The key the relay admits peers with.</param>
        /// <param name="maximumClients">How many clients a hosted room will hold.</param>
        /// <param name="requestedRoomCode">The room a joining peer asks for.</param>
        private RelayLink(bool isHost, string connectionKey, int maximumClients, string requestedRoomCode)
        {
            _isHost = isHost;
            _connectionKey = connectionKey;
            _maximumClients = maximumClients;
            _requestedRoomCode = requestedRoomCode;
        }

        /// <summary>
        /// Opens the socket and starts the handshake. The link is not usable until <see cref="Ready"/> is raised.
        /// </summary>
        /// <param name="relayEndPoint">The relay to connect to.</param>
        /// <param name="maximumTransmissionUnit">The largest datagram to build.</param>
        /// <returns><see langword="true"/> when the socket opened and the connection attempt began.</returns>
        public bool TryConnect(IPEndPoint relayEndPoint, ushort maximumTransmissionUnit)
        {
            SynapseConfig synapseConfig = new()
            {
                BindEndPoints = new List<IPEndPoint> { new IPEndPoint(IPAddress.Any, 0) },

                MaximumTransmissionUnit = maximumTransmissionUnit,

                MaximumPacketSize = maximumTransmissionUnit,

                // The receive engine otherwise hands out a slice of its own reusable buffer, which is gone by the time the
                // engine's loop reads it.
                CopyReceivedPayloads = true,
            };

            /* Room for the largest datagram the socket will build, plus the relay framing that goes around a payload of that
             * size. Allocated once here rather than per send. */
            _sendBuffer = new byte[maximumTransmissionUnit + MaximumFrameHeaderSize];

            _synapseManager = new SynapseManager(synapseConfig);
            _synapseManager.ConnectionEstablished += OnRelayConnectionEstablished;
            _synapseManager.ConnectionClosed += OnRelayConnectionClosed;
            _synapseManager.PacketReceived += OnRelayPacketReceived;

            try
            {
                _synapseManager.Start();

                _relayConnection = _synapseManager.Connect(relayEndPoint);

                return true;
            }
            catch (Exception exception)
            {
                Logger<RelayLink>.LogError($"Could not reach the relay at [{relayEndPoint}]: {exception}");

                return false;
            }
        }

        /// <summary>
        /// Drives the socket, which is what raises every event on this type.
        /// </summary>
        public void Poll()
        {
            _synapseManager?.Poll();
        }

        /// <summary>
        /// Sends game data from a host to one client, or to every client at once.
        /// </summary>
        /// <param name="virtualClientId">The client to send to, or <see cref="MessageCodec.BroadcastVirtualClientId"/> for all.</param>
        /// <param name="channel">The channel to ride.</param>
        /// <param name="payload">The data to send.</param>
        /// <returns><see langword="true"/> when the relay took it.</returns>
        public bool TrySendAsHost(int virtualClientId, Channel channel, ArraySegment<byte> payload)
        {
            if (!HasRoomToFrame(payload.Count))
                return false;

            return TrySend(MessageCodec.WriteHostData(_sendBuffer, virtualClientId, (byte)ToGameChannel(channel), payload), channel);
        }

        /// <summary>
        /// Sends game data from a client to the host.
        /// </summary>
        /// <param name="channel">The channel to ride.</param>
        /// <param name="payload">The data to send.</param>
        /// <returns><see langword="true"/> when the relay took it.</returns>
        public bool TrySendAsClient(Channel channel, ArraySegment<byte> payload)
        {
            if (!HasRoomToFrame(payload.Count))
                return false;

            return TrySend(MessageCodec.WriteClientData(_sendBuffer, (byte)ToGameChannel(channel), payload), channel);
        }

        /// <summary>
        /// Asks the relay to remove a client from the room this peer hosts.
        /// </summary>
        /// <param name="virtualClientId">The client to remove.</param>
        public void KickClient(int virtualClientId)
        {
            TrySend(MessageCodec.WriteKick(_sendBuffer, virtualClientId), Channel.Reliable);
        }

        /// <inheritdoc/>
        public void Dispose()
        {
            if (_synapseManager is null)
                return;

            _synapseManager.ConnectionEstablished -= OnRelayConnectionEstablished;
            _synapseManager.ConnectionClosed -= OnRelayConnectionClosed;
            _synapseManager.PacketReceived -= OnRelayPacketReceived;

            if (_relayConnection is not null)
            {
                _synapseManager.Disconnect(_relayConnection);

                /* Driven so the goodbye actually leaves before the socket does. Without it the relay hears nothing and takes its
                 * own timeout to miss this peer, which for a host means every client sits in a room with a dead host at the top
                 * of it for as long as that takes. */
                _synapseManager.Poll();
            }

            _synapseManager.Stop();
            _synapseManager.Dispose();

            _synapseManager = null;
            _relayConnection = null;

            IsReady = false;
        }

        /// <summary>
        /// Maps an engine channel onto the relay's.
        /// </summary>
        /// <param name="channel">The engine channel.</param>
        /// <returns>The relay channel.</returns>
        private static GameChannel ToGameChannel(Channel channel)
        {
            return channel == Channel.Reliable ? GameChannel.Reliable : GameChannel.Unreliable;
        }

        /// <summary>
        /// Sends whatever was written into the send buffer.
        /// </summary>
        /// <param name="length">How many bytes of the buffer the framed message came to.</param>
        /// <param name="channel">The channel deciding whether the relay hop is reliable.</param>
        /// <returns><see langword="true"/> when the relay took it.</returns>
        /// <remarks>
        /// Every message is framed into the one buffer this link owns, and the segment handed to the socket is a struct over it,
        /// so a send allocates nothing however many of them a tick makes. The buffer is safe to reuse the moment the send
        /// returns: the socket copies what it is given, keeping its own copy of a reliable payload for retransmission.
        /// </remarks>
        private bool TrySend(int length, Channel channel)
        {
            if (_synapseManager is null || _relayConnection is null)
                return false;

            try
            {
                _synapseManager.Send(_relayConnection, new ArraySegment<byte>(_sendBuffer, 0, length), channel == Channel.Reliable);

                return true;
            }
            catch (Exception exception)
            {
                Logger<RelayLink>.LogError($"A send to the relay failed: {exception}");

                return false;
            }
        }

        /// <summary>
        /// Returns whether a payload and the relay's framing around it fit in the send buffer.
        /// </summary>
        /// <param name="payloadLength">The payload to be framed.</param>
        /// <returns><see langword="true"/> when there is room.</returns>
        private bool HasRoomToFrame(int payloadLength)
        {
            if (payloadLength + MaximumFrameHeaderSize <= _sendBuffer.Length)
                return true;

            Logger<RelayLink>.LogError($"A payload of [{payloadLength}] bytes does not fit the relay's framing inside a [{_sendBuffer.Length}] byte datagram.");

            return false;
        }

        /// <summary>
        /// Authenticates, then asks for the role this peer wants.
        /// </summary>
        /// <param name="connectionEventArgs">The established link.</param>
        private void OnRelayConnectionEstablished(ConnectionEventArgs connectionEventArgs)
        {
            if (_isHandshakeSent)
                return;

            _isHandshakeSent = true;
            _relayConnection = connectionEventArgs.Connection;

            TrySend(MessageCodec.WriteAuthenticate(_sendBuffer, _connectionKey), Channel.Reliable);

            // The relay answers a successful authentication with silence, so the role request follows immediately rather than
            // waiting for something that is never sent.
            if (_isHost)
            {
                TrySend(MessageCodec.WriteHostRegister(_sendBuffer, _maximumClients), Channel.Reliable);

                return;
            }

            TrySend(MessageCodec.WriteClientJoin(_sendBuffer, _requestedRoomCode), Channel.Reliable);
        }

        /// <summary>
        /// Reports the link as gone when the connection that closed is the link's own.
        /// </summary>
        /// <param name="connectionEventArgs">The closed connection.</param>
        private void OnRelayConnectionClosed(ConnectionEventArgs connectionEventArgs)
        {
            // Compared by reference because the socket also reports a stranger's connection closing, and that is not this link.
            if (!ReferenceEquals(connectionEventArgs.Connection, _relayConnection))
                return;

            /* Dropped so a send after the close finds no link and reports failure quietly. A closed connection is dead for good,
             * and the socket rejects a send on one. */
            _relayConnection = null;
            IsReady = false;

            Closed?.Invoke();
        }

        /// <summary>
        /// Decodes one relay message.
        /// </summary>
        /// <param name="packetReceivedEventArgs">The message as it arrived.</param>
        private void OnRelayPacketReceived(PacketReceivedEventArgs packetReceivedEventArgs)
        {
            ArraySegment<byte> payload = packetReceivedEventArgs.Payload;

            if (payload.Count == 0 || !MessageCodec.TryReadMessageType(payload, out MessageType messageType))
                return;

            switch (messageType)
            {
                case MessageType.RoomCreated:
                    if (MessageCodec.TryReadRoomCreated(payload, out string roomCode, out string _))
                        MarkReady(roomCode);
                    break;

                case MessageType.JoinSuccess:
                    if (MessageCodec.TryReadJoinSuccess(payload))
                        MarkReady(_requestedRoomCode);
                    break;

                case MessageType.Connected:
                    if (MessageCodec.TryReadConnected(payload, out int joiningVirtualClientId))
                        ClientJoined?.Invoke(joiningVirtualClientId);
                    break;

                case MessageType.Disconnected:
                    if (MessageCodec.TryReadDisconnected(payload, out int leavingVirtualClientId))
                        ClientLeft?.Invoke(leavingVirtualClientId);
                    break;

                case MessageType.Data:
                    HandleData(payload);
                    break;

                case MessageType.Error:
                    Logger<RelayLink>.LogError($"The relay refused this peer: [{ReadErrorCode(payload)}].");

                    IsReady = false;

                    Closed?.Invoke();
                    break;

                default:
                    // Anything else is either for the other role or a message this integration does not use.
                    break;
            }
        }

        /// <summary>
        /// Reports a peer as holding the role it asked for.
        /// </summary>
        /// <param name="roomCode">The room it now holds or has joined.</param>
        private void MarkReady(string roomCode)
        {
            RoomCode = roomCode;
            IsReady = true;

            Ready?.Invoke();
        }

        /// <summary>
        /// Unwraps game data, which is framed differently depending on which role is reading it.
        /// </summary>
        /// <param name="payload">The relay message.</param>
        private void HandleData(ArraySegment<byte> payload)
        {
            if (_isHost)
            {
                if (MessageCodec.TryReadHostDataSpan(payload, out int senderVirtualClientId, out byte gameChannel, out ReadOnlySpan<byte> hostBoundPayload))
                    DataReceived?.Invoke(senderVirtualClientId, ToChannel(gameChannel), CopyOf(hostBoundPayload));

                return;
            }

            if (MessageCodec.TryReadClientDataSpan(payload, out byte clientGameChannel, out ReadOnlySpan<byte> clientBoundPayload))
                DataReceived?.Invoke(HostVirtualClientId, ToChannel(clientGameChannel), CopyOf(clientBoundPayload));
        }

        /// <summary>
        /// Copies a payload the relay only lends for the duration of the callback.
        /// </summary>
        /// <param name="payload">The lent payload.</param>
        /// <returns>A copy the caller may keep.</returns>
        /// <remarks>
        /// Rented from the shared pool rather than allocated, because the engine returns the array behind an incoming packet to
        /// that pool once it has read it. A plain array handed over here is refused by the pool at the moment of return, which
        /// surfaces far away from the transport that produced it.
        /// </remarks>
        private static ArraySegment<byte> CopyOf(ReadOnlySpan<byte> payload)
        {
            byte[] rentedArray = ArrayPool<byte>.Shared.Rent(payload.Length);

            payload.CopyTo(rentedArray);

            // The pool may hand back more than was asked for, so the segment carries the length that was actually written.
            return new ArraySegment<byte>(rentedArray, 0, payload.Length);
        }

        /// <summary>
        /// Maps a relay channel onto the engine's.
        /// </summary>
        /// <param name="gameChannel">The relay channel.</param>
        /// <returns>The engine channel.</returns>
        private static Channel ToChannel(byte gameChannel)
        {
            return (GameChannel)gameChannel == GameChannel.Reliable ? Channel.Reliable : Channel.Unreliable;
        }

        /// <summary>
        /// Reads why the relay refused this peer.
        /// </summary>
        /// <param name="payload">The error message.</param>
        /// <returns>The code, or a placeholder when it could not be read.</returns>
        private static string ReadErrorCode(ArraySegment<byte> payload)
        {
            return MessageCodec.TryReadError(payload, out ErrorCode errorCode) ? errorCode.ToString() : "unreadable";
        }
    }
}
