namespace Nucleus.Integrations.BlitzRelay
{
    /// <summary>
    /// What a peer is currently doing about the session it holds with a directory.
    /// </summary>
    public enum RelayMigrationState : byte
    {
        /// <summary>
        /// The peer holds no session and is doing nothing about one.
        /// </summary>
        Idle,

        /// <summary>
        /// The peer has asked the directory for a session and is waiting to be given one.
        /// </summary>
        OpeningSession,

        /// <summary>
        /// The peer is hosting the session, and the room it holds is the one the directory hands out.
        /// </summary>
        Hosting,

        /// <summary>
        /// The peer is finding the session for the first time.
        /// </summary>
        Joining,

        /// <summary>
        /// The peer is in the room, playing, and watching for the loss of whoever hosts it.
        /// </summary>
        Playing,

        /// <summary>
        /// The peer has lost its host and is waiting for the directory to say who hosts next, or to tell it to.
        /// </summary>
        Waiting,

        /// <summary>
        /// The peer has been told to take the session over and is standing up a room for it.
        /// </summary>
        Promoting,

        /// <summary>
        /// The session could not be carried on, and this peer is no longer part of it.
        /// </summary>
        Abandoned,
    }
}
