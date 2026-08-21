using System.Threading.Tasks;
using CodeBoost.Logging;
using Nucleus.Connections;
using Nucleus.Integrations.Newfarm;
using Nucleus.Transports;

namespace Nucleus.Integrations.BlitzRelay
{
    /// <summary>
    /// Carries a session over a Blitz Relay on behalf of a directory, which is the whole of what this transport contributes to a
    /// handover.
    /// </summary>
    /// <remarks>
    /// A relayed room belongs to the peer that made it and dies with that peer, taking every other peer's link with it, so a
    /// handover is a new room with a name nobody could have known in advance. Everything that follows from that, noticing the
    /// room go, being elected, adopting the world this peer kept, and finding where the session moved to, belongs to
    /// <see cref="NewfarmHostMigration"/> and is the same for any service. This is only the part that is a relay: opening a room,
    /// naming it, joining somebody else's, and leaving.
    /// The credential is the room code, and the tag it is filed under is <see cref="RelayAdapterTag"/>.
    /// </remarks>
    /// <seealso cref="RelayTransport"/>
    public sealed class RelaySessionHost : ISessionHost
    {
        /// <inheritdoc/>
        public string AdapterTag => RelayAdapterTag;

        /// <inheritdoc/>
        public string HostedCredential => _relayTransport.HostedRoomCode;

        /// <inheritdoc/>
        public bool IsHostingLinkUp => IsLinkUp(Invoker.Server);

        /// <inheritdoc/>
        public bool IsJoinedLinkUp => IsLinkUp(Invoker.Client);

        /// <summary>
        /// The tag a room is filed under with a directory, which is what tells a peer the credential it has been handed is a
        /// relay room code rather than some other service's idea of an address.
        /// </summary>
        public const string RelayAdapterTag = "blitzrelay";

        /// <summary>
        /// The transport this stands a session up on.
        /// </summary>
        private readonly RelayTransport _relayTransport;

        /// <summary>
        /// The handshake timeout to give the transport back once the operation that borrowed it is done, or
        /// <see cref="UnsetTimeoutMilliseconds"/> when the transport's own is not on loan.
        /// </summary>
        private uint _lentTimeoutMilliseconds = UnsetTimeoutMilliseconds;

        /// <summary>
        /// The value <see cref="_lentTimeoutMilliseconds"/> holds while the transport's handshake timeout is its own.
        /// </summary>
        private const uint UnsetTimeoutMilliseconds = 0;

        /// <summary>
        /// Creates an adapter over a relay transport.
        /// </summary>
        /// <param name="relayTransport">The transport carrying the session.</param>
        public RelaySessionHost(RelayTransport relayTransport)
        {
            _relayTransport = relayTransport;
        }

        /// <inheritdoc/>
        /// <remarks>
        /// Empty. Both of this transport's sockets are driven by the engine's own loop, and each of them drives its link to the
        /// relay itself while a handshake is outstanding, so there is nothing here that a poll would advance.
        /// </remarks>
        public void Poll() { }

        /// <inheritdoc/>
        public async Task<SessionHostResult> StartHostingAsync(uint timeoutMilliseconds)
        {
            if (await ConnectWithinAsync(Invoker.Server, timeoutMilliseconds) is not ConnectionStateChangeResult.Success)
                return SessionHostResult.LocalFailure;

            if (HostedCredential.Length > 0)
                return SessionHostResult.Success;

            /* Connected with no room named is not hosting, whatever the state change said. Reporting success on it would publish
             * an empty credential to the directory and point every other peer at nothing, so it comes back down instead. */
            Logger<RelaySessionHost>.LogError("The relay reported this peer connected without naming a room for it, so there is nothing another peer could be pointed at.");

            await StopHostingAsync();

            return SessionHostResult.LocalFailure;
        }

        /// <inheritdoc/>
        public async Task<SessionHostResult> JoinAsync(string credential, uint timeoutMilliseconds)
        {
            if (string.IsNullOrEmpty(credential))
            {
                Logger<RelaySessionHost>.LogError("A join was asked for with no room to join, which is this peer's own fault rather than the room's.");

                return SessionHostResult.LocalFailure;
            }

            // Whatever this peer was in is left before another is tried, so a retry never builds on the attempt before it.
            await LeaveAsync();

            _relayTransport.RoomCode = credential;

            if (await ConnectWithinAsync(Invoker.Client, timeoutMilliseconds) is ConnectionStateChangeResult.Success)
                return SessionHostResult.Success;

            /* The relay answering and closing the link is it saying the room is not there, or will not have this peer, and that
             * is the one thing a directory should be told: it is how newfarm learns a host has gone quiet to everybody but
             * itself. Any other failure is this peer's own side, and reporting that as an unreachable room would have a
             * perfectly healthy host challenged and the session handed over for no reason.
             * A relay that says nothing at all falls on the second side of that deliberately. It has not told this peer anything
             * about the room, and a host that genuinely died stops heartbeating anyway, so the directory reaches the same answer
             * on its own without this peer having guessed at it. */
            return _relayTransport.LastJoinOutcome is RelayLinkOutcome.Refused ? SessionHostResult.CredentialUnreachable : SessionHostResult.LocalFailure;
        }

        /// <inheritdoc/>
        public async Task StopHostingAsync()
        {
            if (!_relayTransport.TryGetConnection(Invoker.Server, out Connection _))
                return;

            await _relayTransport.DisconnectLocalConnectionAsync(Invoker.Server);
        }

        /// <inheritdoc/>
        public async Task LeaveAsync()
        {
            if (!_relayTransport.TryGetConnection(Invoker.Client, out Connection _))
                return;

            await _relayTransport.DisconnectLocalConnectionAsync(Invoker.Client);
        }

        /// <summary>
        /// Connects one side of the transport, giving the relay no longer than it has been allowed.
        /// </summary>
        /// <param name="invoker">Which side to connect.</param>
        /// <param name="timeoutMilliseconds">The longest the relay may take, capped by what the transport was already set to.</param>
        /// <returns>The result of the state change.</returns>
        /// <remarks>
        /// The transport's own handshake timeout is what a socket waits on, so honouring a caller's budget means lending the
        /// transport that budget for the length of the call and giving it back in a finally afterwards.
        /// Only the first caller lends, and only the first caller gives back. The coordinator can begin standing a session up
        /// while a join it was asked for is still outstanding, which <see cref="ISessionHost"/> spells out, and a second caller
        /// that captured the borrowed value as though it were the configured one would hand that back at the end. The transport's
        /// own timeout would ratchet down every time it happened, and stay down for the rest of the process.
        /// </remarks>
        private async Task<ConnectionStateChangeResult> ConnectWithinAsync(Invoker invoker, uint timeoutMilliseconds)
        {
            bool isLender = _lentTimeoutMilliseconds == UnsetTimeoutMilliseconds;

            if (isLender)
            {
                _lentTimeoutMilliseconds = _relayTransport.RelayHandshakeTimeoutMilliseconds;

                _relayTransport.RelayHandshakeTimeoutMilliseconds = timeoutMilliseconds < _lentTimeoutMilliseconds ? timeoutMilliseconds : _lentTimeoutMilliseconds;
            }

            try
            {
                return await _relayTransport.ConnectAsync(invoker);
            }
            finally
            {
                if (isLender)
                {
                    _relayTransport.RelayHandshakeTimeoutMilliseconds = _lentTimeoutMilliseconds;
                    _lentTimeoutMilliseconds = UnsetTimeoutMilliseconds;
                }
            }
        }

        /// <summary>
        /// Returns whether one side of the transport is connected.
        /// </summary>
        /// <param name="invoker">Which side to ask about.</param>
        /// <returns><see langword="true"/> when that side is connected.</returns>
        private bool IsLinkUp(Invoker invoker) => _relayTransport.TryGetConnection(invoker, out Connection connection) && connection.LocalState is LocalConnectionState.Connected;
    }
}
