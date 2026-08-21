using System.Net;
using Nucleus.Connections;
using Nucleus.Transports;

namespace Nucleus.Integrations.BlitzRelay
{
    /// <summary>
    /// A <see cref="Transport"/> that carries a session through a Blitz Relay server, so peers reach each other without any of
    /// them being reachable.
    /// </summary>
    /// <remarks>
    /// Nothing here listens. Both sides are clients of the relay, one holding the host role in a room and the rest joining it,
    /// which is what lets a session form between peers that are all behind something. What replaces an address is
    /// <see cref="RoomCode"/>: the authority connects, the relay names a room, and every other peer needs that name and nothing
    /// else.
    /// The room belongs to the peer that made it and dies with that peer, so the code changes at every handover. That is what
    /// <see cref="RelayHostMigration"/> is for.
    /// </remarks>
    public class RelayTransport : SocketPairTransport<RelayServerSocket, RelayClientSocket>
    {
        /// <summary>
        /// The relay to carry this session.
        /// </summary>
        public IPEndPoint RelayEndPoint { get; set; } = new IPEndPoint(IPAddress.Loopback, DefaultRelayPort);

        /// <summary>
        /// The key the relay admits peers with, which is the relay operator's, not a player's.
        /// </summary>
        public string ConnectionKey { get; set; } = string.Empty;

        /// <summary>
        /// The room to join, and after the authority connects, the room it created.
        /// </summary>
        /// <remarks>
        /// Read by a joining peer, so set it before connecting a client. Written by the authority's side once the relay names its
        /// room, so read it after connecting a server: that value is what a joining peer needs, and there is no other way to
        /// reach this session.
        /// </remarks>
        public string RoomCode
        {
            get => ServerSocket is not null && ServerSocket.RoomCode.Length > 0 ? ServerSocket.RoomCode : _roomCode;
            set => _roomCode = value;
        }

        /// <summary>
        /// How long to wait for the relay to answer a handshake before giving up on it.
        /// </summary>
        public uint RelayHandshakeTimeoutMilliseconds { get; set; } = 10000;

        /// <summary>
        /// How many clients the room this peer creates will hold.
        /// </summary>
        /// <remarks>
        /// A room has to be given a size when it is made, and the relay refuses one without a real number, so
        /// <see cref="ServerConfiguration"/>.MaximumConnections being left unset cannot mean "no limit" here the way it does on
        /// a transport that listens. That setting is used when it has been given a value, and this stands in when it has not.
        /// </remarks>
        public int MaximumClients { get; set; } = DefaultMaximumClients;

        /// <summary>
        /// The room size used when nothing else says one.
        /// </summary>
        public const int DefaultMaximumClients = 16;

        /// <summary>
        /// The size to ask the relay for, being whatever the server configuration says if it says anything.
        /// </summary>
        internal int RoomSize => ServerConfiguration.MaximumConnections is ServerConfiguration.UnsetMaximumConnections ? MaximumClients : (int)ServerConfiguration.MaximumConnections;

        /// <summary>
        /// The port a Blitz Relay listens on unless it was told otherwise.
        /// </summary>
        public const int DefaultRelayPort = 7770;

        /// <summary>
        /// The server side's own Connection, which the client side hands to the engine as the sender of inbound packets.
        /// </summary>
        internal Connection AuthorityStandIn => ServerSocket.Connection;

        /// <summary>
        /// The room a joining peer was told to join.
        /// </summary>
        private string _roomCode = string.Empty;

        /// <inheritdoc/>
        /// <remarks>
        /// The relay's own framing sits between the engine's packet and the datagram carrying it, so the engine has to leave room
        /// for it or the socket layer chops what the engine built. This is the widest of them, being a host's data frame: the
        /// message type, the client it is addressed to, and the channel it rides.
        /// </remarks>
        public override uint DatagramOverheadBytes => RelayDataHeaderBytes + SynapseDatagramOverheadBytes;

        /// <summary>
        /// The relay's own header on a host data frame: a message type, a virtual client id, and a channel.
        /// </summary>
        private const uint RelayDataHeaderBytes = 6;

        /// <summary>
        /// The widest header the socket layer beneath the relay puts on a datagram.
        /// </summary>
        /// <remarks>
        /// Named rather than read from the socket library, because this transport reaches the relay through its own copy of that
        /// library and the engine's copy is aliased away from anything outside it.
        /// </remarks>
        private const uint SynapseDatagramOverheadBytes = 16;

        /// <inheritdoc/>
        /// <remarks>A relayed session is a real network path, even when the relay happens to run on this machine, so the security checks stay on.</remarks>
        public override bool IsLocalTransport() => false;
    }
}
