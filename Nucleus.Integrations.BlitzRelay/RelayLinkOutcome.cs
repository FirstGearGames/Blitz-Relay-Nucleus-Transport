namespace Nucleus.Integrations.BlitzRelay
{
    /// <summary>
    /// How an attempt to take a link to the relay ended.
    /// </summary>
    /// <remarks>
    /// Kept because a joining peer's caller has to tell the two failures apart. A relay that answered and would not admit this
    /// peer has said the room is not there, which is what a directory needs to hear; a relay this peer could not reach at all
    /// has said nothing about the room, and passing that on as an unreachable room gets a healthy host challenged.
    /// </remarks>
    internal enum RelayLinkOutcome : byte
    {
        /// <summary>
        /// The relay admitted this peer.
        /// </summary>
        Connected,

        /// <summary>
        /// The relay answered and closed the link, which is what a room that is not there, or will not have this peer, looks
        /// like from here.
        /// </summary>
        Refused,

        /// <summary>
        /// The relay could not be reached, was not answered by, or was never asked because this peer was not configured to ask.
        /// </summary>
        Unavailable,
    }
}
