package uploadadd

import "github.com/storacha/go-ucanto/core/receipt"

func NewReceiptReader() (receipt.ReceiptReader[*Success, *Failure], error) {
	return receipt.NewReceiptReader[*Success, *Failure](ResultSchema)
}
