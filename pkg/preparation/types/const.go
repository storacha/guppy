package types

// MinPiecePayload is the minimum payload size for a piece CID to be computed.
// technically, 65 will work for go-fil-commp-hashhash, but 127 is the minimum for
// commcid.DataCommitmentToPieceCidv2 to succeed. (and this number isn't public)
const MinPiecePayload = 127
