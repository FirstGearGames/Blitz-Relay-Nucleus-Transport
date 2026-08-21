using System;
using System.Diagnostics;
using System.Net;
using System.Threading.Tasks;
using CodeBoost.Logging;
using Newfarm.Client;
using Nucleus.Connections;
using Nucleus.Managers.Client;
using Nucleus.Managers.Core;
using Nucleus.Transports;

namespace Nucleus.Integrations.BlitzRelay
{
    /// <summary>
    /// Keeps a relayed session alive across the loss of whoever was hosting it, by registering the session with a newfarm
    /// directory and doing what the directory says when the host goes.
    /// </summary>
    /// <remarks>
    /// A relayed room belongs to the peer that created it and dies with that peer, taking every other peer's link with it. So a
    /// handover is not a reconnection: it is a new room, with a name nobody could have known in advance, and no channel left
    /// between the survivors to tell them what it is. The directory is the out-of-band answer to that, and this is the piece that
    /// drives it: it holds the session, notices the room dying, and either takes the session over or waits to be told where it
    /// went.
    /// The engine's own half is already there. A client set to <see cref="DisconnectResetMode.RetainReceivedWorld"/> keeps the
    /// world when its link drops, and <see cref="Managers.Server.ServerManager.AdoptRetainedWorld"/> makes that world its own to
    /// serve. This adds the finding of each other, and nothing else.
    /// Poll driven: nothing here owns a thread, and every event it raises comes out of <see cref="Poll"/>.
    /// </remarks>
    public sealed class RelayHostMigration : IDisposable
    {
        /// <summary>
        /// Raised on the peer that has been told to take the session over, before it does. The session is not hosted again until
        /// this returns, so a game that has to stand something up first does it here.
        /// </summary>
        public event Action? Promoting;

        /// <summary>
        /// Raised once this peer is hosting the session, carrying the room the relay named. Distribute nothing: every other peer
        /// is told by the directory.
        /// </summary>
        public event Action<string>? Promoted;

        /// <summary>
        /// Raised on a surviving peer once it has rejoined the session in its new room.
        /// </summary>
        public event Action<string>? Rejoined;

        /// <summary>
        /// Raised when the session could not be carried on, whether the directory refused it or no peer would host it.
        /// </summary>
        public event Action<string>? Abandoned;

        /// <summary>
        /// Names the session with the directory. This is what a host distributes to its clients, and the only thing a peer needs
        /// to find its way back.
        /// </summary>
        /// <seealso cref="Managers.Server.SessionDirectoryMessage"/>
        public ulong SessionId => _identity.SessionId;

        /// <summary>
        /// What this peer is currently doing about the session.
        /// </summary>
        public RelayMigrationState State { get; private set; } = RelayMigrationState.Idle;

        /// <summary>
        /// The tag the room is filed under with the directory, which is what tells a peer the credential is a relay room code
        /// rather than some other service's idea of an address.
        /// </summary>
        public const string RelayAdapterTag = "blitzrelay";

        /// <summary>
        /// The engine this is keeping a session for.
        /// </summary>
        private readonly CoreManager _coreManager;

        /// <summary>
        /// The transport carrying the session.
        /// </summary>
        private readonly RelayTransport _relayTransport;

        /// <summary>
        /// This peer's link to the directory.
        /// </summary>
        private readonly NewfarmClient _directory;

        /// <summary>
        /// The session this peer holds, once it has one.
        /// </summary>
        private NewfarmSessionIdentity _identity;

        /// <summary>
        /// True once the transport has been seen to lose its client link, so the loss is acted on once.
        /// </summary>
        private bool _isHandlingHostLoss;

        /// <summary>
        /// True when the directory has told this peer to host and the next poll should act on it.
        /// </summary>
        private bool _isPromotionPending;

        /// <summary>
        /// The room being opened for a promotion, or <see langword="null"/> when none is in flight.
        /// </summary>
        private Task<ConnectionStateChangeResult>? _promotionTask;

        /// <summary>
        /// The room the directory has said the session moved to, waiting for the next poll to join it.
        /// </summary>
        private string? _pendingRoomCode;

        /// <summary>
        /// The room currently being joined, or <see langword="null"/> when none is.
        /// </summary>
        private string? _rejoiningRoomCode;

        /// <summary>
        /// The join in flight, or <see langword="null"/> when none is.
        /// </summary>
        private Task<ConnectionStateChangeResult>? _rejoinTask;

        /// <summary>
        /// The room this peer was in when it lost its host, so an answer naming that same room is known for what it is.
        /// </summary>
        private string? _lostRoomCode;

        /// <summary>
        /// Creates a coordinator for a session.
        /// </summary>
        /// <param name="coreManager">The engine keeping the session.</param>
        /// <param name="relayTransport">The transport carrying it.</param>
        /// <param name="directoryEndPoint">The newfarm directory to register with.</param>
        public RelayHostMigration(CoreManager coreManager, RelayTransport relayTransport, IPEndPoint directoryEndPoint)
        {
            _coreManager = coreManager;
            _relayTransport = relayTransport;

            _directory = new NewfarmClient(new NewfarmClientConfig(directoryEndPoint));
            _directory.SessionCreated += OnSessionCreated;
            _directory.ElectedToHost += OnElectedToHost;
            _directory.CredentialAvailable += OnCredentialAvailable;
            _directory.HostingChallenged += OnHostingChallenged;
            _directory.HostingRevoked += OnHostingRevoked;
            _directory.Refused += OnRefused;
        }

        /// <summary>
        /// Opens a session, starts hosting it, and registers the room the relay names.
        /// </summary>
        /// <param name="timeoutMilliseconds">How long to wait for the directory and the relay together.</param>
        /// <returns><see langword="true"/> when the session is open and being hosted.</returns>
        public async Task<bool> StartHostingAsync(uint timeoutMilliseconds = DefaultTimeoutMilliseconds)
        {
            State = RelayMigrationState.OpeningSession;

            _directory.CreateSession();

            if (!await WaitUntilAsync(() => _identity.SessionId is not 0, timeoutMilliseconds))
            {
                Fail("The directory did not open a session.");

                return false;
            }

            if (await _relayTransport.ConnectAsync(Invoker.Server) is not ConnectionStateChangeResult.Success)
            {
                Fail("The relay would not open a room for this peer.");

                return false;
            }

            PublishRoom();

            State = RelayMigrationState.Hosting;

            return true;
        }

        /// <summary>
        /// Finds a session by its identifier and joins whatever room it currently lives in.
        /// </summary>
        /// <param name="sessionId">The session, as the host distributed it.</param>
        /// <param name="timeoutMilliseconds">How long to wait for the directory and the relay together.</param>
        /// <returns><see langword="true"/> when this peer is in the session, whether as a client of its room or, having been elected while asking, as its host.</returns>
        /// <remarks>
        /// The identifier is all a peer needs. It does not have to be told a room code, and being told one would be worse than
        /// useless after a handover, since the room it names is the one that just died. A join ending in this peer's own
        /// election is a success: <see cref="Promoted"/> narrates it, and <see cref="State"/> answers which chair the peer took.
        /// </remarks>
        public async Task<bool> JoinAsync(ulong sessionId, uint timeoutMilliseconds = DefaultTimeoutMilliseconds)
        {
            State = RelayMigrationState.Joining;

            _identity = new NewfarmSessionIdentity(sessionId, epoch: 0);

            string joinedRoomCode = string.Empty;

            void CaptureRoom(NewfarmCredential credential) => joinedRoomCode = credential.Credential;

            _directory.CredentialAvailable += CaptureRoom;

            try
            {
                _directory.AwaitSession(_identity);

                Stopwatch joinStopwatch = Stopwatch.StartNew();

                /* The ask ends four ways, and only one of them is joining the room the directory first names. The directory may
                 * refuse, the session not existing, which the refusal handler reports the moment it happens; waiting out the
                 * timeout to say it again is only noise. The room it names may be dead, its host having died in it, in which
                 * case the join is refused fast and this peer says so and keeps asking, because the directory's next move is
                 * another credential or an election. And the election may land on this very peer, a returning host often being
                 * the only peer its session has left: the ordinary promotion runs from Poll and this peer ends up hosting the
                 * session it asked to join, which is the join succeeding from the other chair. Waiting on for a credential
                 * there would capture this peer's own published room and spend a handshake timeout knocking on its own door. */
                while (true)
                {
                    uint elapsedMilliseconds = (uint)joinStopwatch.ElapsedMilliseconds;

                    if (elapsedMilliseconds >= timeoutMilliseconds || !await WaitUntilAsync(() => joinedRoomCode.Length > 0 || State is RelayMigrationState.Hosting or RelayMigrationState.Abandoned, timeoutMilliseconds - elapsedMilliseconds))
                    {
                        Fail("The directory did not say where the session lives.");

                        return false;
                    }

                    // Elected and promoted while asking: this peer is in the session, from the hosting chair. Promoted has said so.
                    if (State is RelayMigrationState.Hosting)
                        return true;

                    // Refused: the failure was reported once by the refusal itself, and there is nothing left to wait for.
                    if (State is RelayMigrationState.Abandoned)
                        return false;

                    string namedRoomCode = joinedRoomCode;
                    joinedRoomCode = string.Empty;

                    if (await JoinRoomAsync(namedRoomCode))
                        return true;

                    /* The room it named is not there or will not have this peer. Saying so is what moves the directory on to its
                     * next answer, and for a session whose host died in that room, its next answer is an election. */
                    _directory.ReportCredentialUnreachable();
                }
            }
            finally
            {
                _directory.CredentialAvailable -= CaptureRoom;
            }
        }

        /// <summary>
        /// Drives the directory and watches the transport for the loss of its host. Call once a frame, or once a tick.
        /// </summary>
        public void Poll()
        {
            _directory.Poll();

            if (_promotionTask is not null)
            {
                if (_promotionTask.IsCompleted)
                    CompletePromotion();

                return;
            }

            if (_isPromotionPending)
            {
                BeginPromotion();

                return;
            }

            if (_rejoinTask is not null)
            {
                if (_rejoinTask.IsCompleted)
                    CompleteRejoin();

                return;
            }

            if (_pendingRoomCode is not null)
            {
                BeginRejoin();

                return;
            }

            if (State is not RelayMigrationState.Playing || _isHandlingHostLoss)
                return;

            // The relay tears the room down with its host, so a client link that has gone is what a lost host looks like here.
            if (_relayTransport.TryGetConnection(Invoker.Client, out Connection clientConnection) && clientConnection.LocalState is LocalConnectionState.Connected)
                return;

            _isHandlingHostLoss = true;
            _lostRoomCode = _relayTransport.RoomCode;
            State = RelayMigrationState.Waiting;

            /* The directory may well not have missed the host yet, in which case it answers with the room that just died. That is
             * the right answer to a peer whose own link merely blipped, so the wrong one here is only found out by trying it,
             * which is what ReportCredentialUnreachable exists for. */
            _directory.AwaitSession(_identity);
        }

        /// <inheritdoc/>
        public void Dispose()
        {
            _directory.SessionCreated -= OnSessionCreated;
            _directory.ElectedToHost -= OnElectedToHost;
            _directory.CredentialAvailable -= OnCredentialAvailable;
            _directory.HostingChallenged -= OnHostingChallenged;
            _directory.HostingRevoked -= OnHostingRevoked;
            _directory.Refused -= OnRefused;

            _directory.Dispose();
        }

        /// <summary>
        /// How long the directory and the relay are given together, unless the caller says otherwise.
        /// </summary>
        private const uint DefaultTimeoutMilliseconds = 15000;

        /// <summary>
        /// Adopts a newly opened session.
        /// </summary>
        /// <param name="identity">The identity the directory issued.</param>
        private void OnSessionCreated(NewfarmSessionIdentity identity)
        {
            _identity = identity;
        }

        /// <summary>
        /// Records that this peer has been told to take the session over. The taking over itself happens in <see cref="Poll"/>.
        /// </summary>
        /// <param name="epoch">The epoch this peer was elected for.</param>
        /// <remarks>
        /// Deliberately does nothing but set a flag. This is raised from inside a poll of the directory, and promoting touches
        /// the engine: adopting a world, starting a server, and every callback those raise. Doing that here through an async
        /// method would resume the second half of it on whatever thread the wait completed on, which is not the thread the
        /// engine is driven from. <see cref="Poll"/> is that thread by definition, so the work belongs there.
        /// </remarks>
        private void OnElectedToHost(uint epoch)
        {
            if (State is RelayMigrationState.Hosting)
                return;

            _isPromotionPending = true;
        }

        /// <summary>
        /// Takes the session over: adopts the world this peer kept, stands a room of its own up, and tells the directory where
        /// it is.
        /// </summary>
        private void BeginPromotion()
        {
            _isPromotionPending = false;

            State = RelayMigrationState.Promoting;

            Promoting?.Invoke();

            /* The world this peer kept is still the previous authority's until it says otherwise, and adopting has to happen
             * before a server starts: being the authority changes what half the engine's passes mean, from the moment the server
             * socket connects. Done here, on the polling thread, because it is the engine's. */
            if (!_coreManager.ServerManager.AdoptRetainedWorld())
                Logger<RelayHostMigration>.LogInformation("Nothing was retained to adopt, so this peer hosts a session it did not keep.");

            _promotionTask = _relayTransport.ConnectAsync(Invoker.Server);
        }

        /// <summary>
        /// Finishes a promotion whose room has been opened, or hands the session on when the relay would not open one.
        /// </summary>
        private void CompletePromotion()
        {
            ConnectionStateChangeResult connectionStateChangeResult = _promotionTask!.GetAwaiter().GetResult();

            _promotionTask = null;

            if (connectionStateChangeResult is not ConnectionStateChangeResult.Success)
            {
                // Somebody else can host, and saying so hands it on at once rather than making everybody wait out the deadline.
                _directory.DeclineElection();

                State = RelayMigrationState.Waiting;

                return;
            }

            PublishRoom();

            State = RelayMigrationState.Hosting;

            Promoted?.Invoke(_relayTransport.RoomCode);
        }

        /// <summary>
        /// Takes note of where the session has moved to, for the next <see cref="Poll"/> to act on.
        /// </summary>
        /// <param name="credential">Where the session now lives.</param>
        /// <remarks>
        /// Raised from inside a poll of the directory, and joining a room ends in engine state changes, so this only records the
        /// room for the same reason <see cref="OnElectedToHost"/> only records an election.
        /// </remarks>
        private void OnCredentialAvailable(NewfarmCredential credential)
        {
            if (State is not RelayMigrationState.Waiting)
                return;

            if (!string.Equals(credential.AdapterTag, RelayAdapterTag, StringComparison.Ordinal))
            {
                Logger<RelayHostMigration>.LogError($"The session moved to a service this peer does not speak: [{credential.AdapterTag}].");

                return;
            }

            /* The directory answers a peer that has only just lost its host with the room that peer was in, because from where it
             * stands the host has not been quiet long enough to be gone. That answer is right for a peer whose own link merely
             * blipped and useless for one whose host has died, and this peer knows which it is: it was in that room a moment ago
             * and watched it go. Saying so at once, rather than spending a handshake timeout proving it, is what moves the
             * directory on to electing somebody. */
            if (string.Equals(credential.Credential, _lostRoomCode, StringComparison.Ordinal))
            {
                _directory.ReportCredentialUnreachable();

                return;
            }

            _pendingRoomCode = credential.Credential;
        }

        /// <summary>
        /// Joins the room the session has moved to.
        /// </summary>
        private void BeginRejoin()
        {
            _rejoiningRoomCode = _pendingRoomCode;
            _pendingRoomCode = null;

            _relayTransport.RoomCode = _rejoiningRoomCode!;

            _rejoinTask = _relayTransport.ConnectAsync(Invoker.Client);
        }

        /// <summary>
        /// Finishes a rejoin, or reports the room unreachable when it would not have this peer.
        /// </summary>
        private void CompleteRejoin()
        {
            ConnectionStateChangeResult connectionStateChangeResult = _rejoinTask!.GetAwaiter().GetResult();

            _rejoinTask = null;

            if (connectionStateChangeResult is not ConnectionStateChangeResult.Success)
            {
                /* The room it named is not there, or will not have this peer. Saying so is what lets the directory tell a host
                 * that cannot host from a peer that cannot reach one. */
                _directory.ReportCredentialUnreachable();

                _rejoiningRoomCode = null;

                return;
            }

            _isHandlingHostLoss = false;
            _lostRoomCode = null;

            State = RelayMigrationState.Playing;

            Rejoined?.Invoke(_rejoiningRoomCode!);

            _rejoiningRoomCode = null;
        }

        /// <summary>
        /// Answers the directory's demand that this peer prove it is still hosting.
        /// </summary>
        /// <param name="epoch">The epoch the session is being hosted for.</param>
        /// <remarks>
        /// Answered by republishing the room this peer holds. If the room has gone, so has the ability to host, and the right
        /// answer is to give the session up rather than to claim a room that is not there.
        /// </remarks>
        private void OnHostingChallenged(uint epoch)
        {
            if (State is RelayMigrationState.Hosting && _relayTransport.RoomCode.Length > 0 && IsServerConnected())
            {
                PublishRoom();

                return;
            }

            _directory.SurrenderHosting();

            State = RelayMigrationState.Waiting;
        }

        /// <summary>
        /// Steps down, the directory having decided this peer is not hosting anything reachable.
        /// </summary>
        /// <param name="epoch">The epoch in force when the session was taken.</param>
        private void OnHostingRevoked(uint epoch)
        {
            State = RelayMigrationState.Waiting;
        }

        /// <summary>
        /// Reports a refusal from the directory, which ends the session for this peer.
        /// </summary>
        /// <param name="newfarmRefusalReason">Why the directory refused.</param>
        private void OnRefused(Newfarm.Wire.NewfarmRefusalReason newfarmRefusalReason)
        {
            Fail($"The directory refused this peer: [{newfarmRefusalReason}].");
        }

        /// <summary>
        /// Tells the directory where this peer's room is.
        /// </summary>
        private void PublishRoom()
        {
            _directory.PublishCredential(RelayAdapterTag, _relayTransport.RoomCode);
        }

        /// <summary>
        /// Joins a room and, on success, resumes watching for the loss of whoever is hosting it.
        /// </summary>
        /// <param name="roomCode">The room to join.</param>
        /// <returns><see langword="true"/> when this peer is in the room.</returns>
        private async Task<bool> JoinRoomAsync(string roomCode)
        {
            _relayTransport.RoomCode = roomCode;

            if (await _relayTransport.ConnectAsync(Invoker.Client) is not ConnectionStateChangeResult.Success)
                return false;

            _isHandlingHostLoss = false;
            State = RelayMigrationState.Playing;

            return true;
        }

        /// <summary>
        /// Returns whether this peer's own server side is up.
        /// </summary>
        /// <returns><see langword="true"/> when it is hosting a room.</returns>
        private bool IsServerConnected()
        {
            return _relayTransport.TryGetConnection(Invoker.Server, out Connection serverConnection) && serverConnection.LocalState is LocalConnectionState.Connected;
        }

        /// <summary>
        /// Reports that the session could not be carried on.
        /// </summary>
        /// <param name="reason">What went wrong.</param>
        private void Fail(string reason)
        {
            State = RelayMigrationState.Abandoned;

            Abandoned?.Invoke(reason);
        }

        /// <summary>
        /// Drives the directory until a condition holds, or the wait runs out.
        /// </summary>
        /// <param name="condition">What is being waited for.</param>
        /// <param name="timeoutMilliseconds">How long to wait.</param>
        /// <returns><see langword="true"/> when the condition held in time.</returns>
        private async Task<bool> WaitUntilAsync(Func<bool> condition, uint timeoutMilliseconds)
        {
            const int PollIntervalMilliseconds = 5;

            for (uint elapsedMilliseconds = 0; elapsedMilliseconds < timeoutMilliseconds; elapsedMilliseconds += PollIntervalMilliseconds)
            {
                _directory.Poll();

                if (condition())
                    return true;

                await Task.Delay(PollIntervalMilliseconds);
            }

            _directory.Poll();

            return condition();
        }
    }
}
