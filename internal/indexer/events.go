package indexer

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"math/big"
	"strings"
	"time"

	"github.com/ethereum/go-ethereum"
	"github.com/ethereum/go-ethereum/accounts/abi"
	"github.com/ethereum/go-ethereum/common"
	ethtypes "github.com/ethereum/go-ethereum/core/types"
	"gorm.io/gorm"

	"github.com/taskmarket/indexer/internal/db"
)

var statusMap = map[uint8]string{
	0: "OPEN",
	1: "ASSIGNED",
	2: "IN_PROGRESS",
	3: "UNDER_REVIEW",
	4: "COMPLETED",
	5: "DISPUTED",
	6: "CANCELLED",
}

// eventHandler wraps the ABI, contract address, and DB for event processing.
type eventHandler struct {
	contractABI  abi.ABI
	contractAddr common.Address
	gdb          *gorm.DB
}

// shortenAddr truncates a hex address for readable log output: 0x1234…cdef
func shortenAddr(addr string) string {
	addr = strings.ToLower(addr)
	if len(addr) > 12 {
		return addr[:8] + "…" + addr[len(addr)-4:]
	}
	return addr
}

// filterLogs fetches logs for a single event from the given block range.
func (h *eventHandler) filterLogs(
	ctx context.Context,
	client ethereum.LogFilterer,
	eventName string,
	from, to uint64,
) ([]ethtypes.Log, error) {
	event, ok := h.contractABI.Events[eventName]
	if !ok {
		return nil, fmt.Errorf("event %q not found in ABI", eventName)
	}
	query := ethereum.FilterQuery{
		FromBlock: new(big.Int).SetUint64(from),
		ToBlock:   new(big.Int).SetUint64(to),
		Addresses: []common.Address{h.contractAddr},
		Topics:    [][]common.Hash{{event.ID}},
	}
	logs, err := client.FilterLogs(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("FilterLogs %s [%d-%d]: %w", eventName, from, to, err)
	}
	return logs, nil
}

// handleTaskCreated processes TaskCreated events.
// Topics[1] taskId, Topics[2] client; Data: reward, deadline, metadataHash
func (h *eventHandler) handleTaskCreated(
	ctx context.Context,
	client ethereum.LogFilterer,
	chainID int64,
	from, to uint64,
) (int, error) {
	logs, err := h.filterLogs(ctx, client, "TaskCreated", from, to)
	if err != nil {
		return 0, err
	}

	type nonIndexed struct {
		Reward       *big.Int
		Deadline     *big.Int
		MetadataHash [32]byte
	}

	processed := 0
	for _, log := range logs {
		if len(log.Topics) < 3 {
			continue
		}
		var d nonIndexed
		if err := h.contractABI.UnpackIntoInterface(&d, "TaskCreated", log.Data); err != nil {
			return processed, fmt.Errorf("unpack TaskCreated: %w", err)
		}

		taskID := new(big.Int).SetBytes(log.Topics[1].Bytes()).Int64()
		clientAddr := common.BytesToAddress(log.Topics[2].Bytes()).Hex()
		deadline := time.Unix(d.Deadline.Int64(), 0).UTC()
		metadataHash := "0x" + fmt.Sprintf("%x", d.MetadataHash)
		rewardWei := d.Reward.String()

		if err := db.UpsertTaskCreated(ctx, h.gdb, chainID, taskID, clientAddr, rewardWei, deadline, metadataHash); err != nil {
			return processed, err
		}
		slog.Debug("TaskCreated",
			"chain", chainID, "id", taskID,
			"client", shortenAddr(clientAddr), "reward_wei", rewardWei,
			"block", log.BlockNumber,
		)
		processed++
	}
	if processed > 0 {
		slog.Info("processed TaskCreated", "chain", chainID, "count", processed, "from", from, "to", to)
	}
	return processed, nil
}

// handleTaskAssigned processes TaskAssigned events.
// Topics[1] taskId, Topics[2] executor
func (h *eventHandler) handleTaskAssigned(
	ctx context.Context,
	client ethereum.LogFilterer,
	chainID int64,
	from, to uint64,
) (int, error) {
	logs, err := h.filterLogs(ctx, client, "TaskAssigned", from, to)
	if err != nil {
		return 0, err
	}

	processed := 0
	for _, log := range logs {
		if len(log.Topics) < 3 {
			continue
		}
		taskID := new(big.Int).SetBytes(log.Topics[1].Bytes()).Int64()
		executor := common.BytesToAddress(log.Topics[2].Bytes()).Hex()

		if err := db.UpdateTaskAssigned(ctx, h.gdb, chainID, taskID, executor); err != nil {
			if errors.Is(err, db.ErrNotFound) {
				slog.Warn("TaskAssigned: task not in DB, skipping",
					"chain", chainID, "id", taskID, "block", log.BlockNumber)
				continue
			}
			return processed, err
		}
		slog.Debug("TaskAssigned",
			"chain", chainID, "id", taskID,
			"executor", shortenAddr(executor), "block", log.BlockNumber,
		)
		processed++
	}
	if processed > 0 {
		slog.Info("processed TaskAssigned", "chain", chainID, "count", processed, "from", from, "to", to)
	}
	return processed, nil
}

// handleTaskStatusChanged processes TaskStatusChanged events.
// Topics[1] taskId; Data: oldStatus, newStatus
func (h *eventHandler) handleTaskStatusChanged(
	ctx context.Context,
	client ethereum.LogFilterer,
	chainID int64,
	from, to uint64,
) (int, error) {
	logs, err := h.filterLogs(ctx, client, "TaskStatusChanged", from, to)
	if err != nil {
		return 0, err
	}

	type nonIndexed struct {
		OldStatus uint8
		NewStatus uint8
	}

	processed := 0
	for _, log := range logs {
		if len(log.Topics) < 2 {
			continue
		}
		var d nonIndexed
		if err := h.contractABI.UnpackIntoInterface(&d, "TaskStatusChanged", log.Data); err != nil {
			return processed, fmt.Errorf("unpack TaskStatusChanged: %w", err)
		}

		taskID := new(big.Int).SetBytes(log.Topics[1].Bytes()).Int64()
		status, ok := statusMap[d.NewStatus]
		if !ok {
			slog.Warn("TaskStatusChanged: unknown status code",
				"chain", chainID, "id", taskID, "code", d.NewStatus)
			continue
		}

		if err := db.UpdateTaskStatus(ctx, h.gdb, chainID, taskID, status); err != nil {
			if errors.Is(err, db.ErrNotFound) {
				slog.Warn("TaskStatusChanged: task not in DB, skipping",
					"chain", chainID, "id", taskID, "status", status)
				continue
			}
			return processed, err
		}
		slog.Debug("TaskStatusChanged",
			"chain", chainID, "id", taskID,
			"old", statusMap[d.OldStatus], "new", status, "block", log.BlockNumber,
		)
		processed++
	}
	if processed > 0 {
		slog.Info("processed TaskStatusChanged", "chain", chainID, "count", processed, "from", from, "to", to)
	}
	return processed, nil
}

// handleCompletionConfirmed processes CompletionConfirmed events.
// Topics[1] taskId, Topics[2] confirmer; Data: clientConfirmed, executorConfirmed
func (h *eventHandler) handleCompletionConfirmed(
	ctx context.Context,
	client ethereum.LogFilterer,
	chainID int64,
	from, to uint64,
) (int, error) {
	logs, err := h.filterLogs(ctx, client, "CompletionConfirmed", from, to)
	if err != nil {
		return 0, err
	}

	type nonIndexed struct {
		ClientConfirmed   bool
		ExecutorConfirmed bool
	}

	processed := 0
	for _, log := range logs {
		if len(log.Topics) < 2 {
			continue
		}
		var d nonIndexed
		if err := h.contractABI.UnpackIntoInterface(&d, "CompletionConfirmed", log.Data); err != nil {
			return processed, fmt.Errorf("unpack CompletionConfirmed: %w", err)
		}

		taskID := new(big.Int).SetBytes(log.Topics[1].Bytes()).Int64()
		confirmer := common.BytesToAddress(log.Topics[2].Bytes()).Hex()

		if err := db.UpdateConfirmations(ctx, h.gdb, chainID, taskID, d.ClientConfirmed, d.ExecutorConfirmed); err != nil {
			if errors.Is(err, db.ErrNotFound) {
				slog.Warn("CompletionConfirmed: task not in DB, skipping",
					"chain", chainID, "id", taskID)
				continue
			}
			return processed, err
		}
		slog.Debug("CompletionConfirmed",
			"chain", chainID, "id", taskID,
			"confirmer", shortenAddr(confirmer),
			"client_ok", d.ClientConfirmed, "executor_ok", d.ExecutorConfirmed,
			"block", log.BlockNumber,
		)
		processed++
	}
	if processed > 0 {
		slog.Info("processed CompletionConfirmed", "chain", chainID, "count", processed, "from", from, "to", to)
	}
	return processed, nil
}

// handleTaskCompleted stores the executor payout and platform fee.
// Topics[1] taskId, Topics[2] executor; Data: payout, fee
func (h *eventHandler) handleTaskCompleted(
	ctx context.Context,
	client ethereum.LogFilterer,
	chainID int64,
	from, to uint64,
) (int, error) {
	logs, err := h.filterLogs(ctx, client, "TaskCompleted", from, to)
	if err != nil {
		return 0, err
	}

	type nonIndexed struct {
		Payout *big.Int
		Fee    *big.Int
	}

	processed := 0
	for _, log := range logs {
		if len(log.Topics) < 2 {
			continue
		}
		var d nonIndexed
		if err := h.contractABI.UnpackIntoInterface(&d, "TaskCompleted", log.Data); err != nil {
			return processed, fmt.Errorf("unpack TaskCompleted: %w", err)
		}

		taskID := new(big.Int).SetBytes(log.Topics[1].Bytes()).Int64()

		if err := db.UpdateTaskCompleted(ctx, h.gdb, chainID, taskID, d.Payout.String(), d.Fee.String()); err != nil {
			if errors.Is(err, db.ErrNotFound) {
				slog.Warn("TaskCompleted: task not in DB, skipping",
					"chain", chainID, "id", taskID)
				continue
			}
			return processed, err
		}
		slog.Debug("TaskCompleted",
			"chain", chainID, "id", taskID,
			"payout", d.Payout.String(), "fee", d.Fee.String(),
			"block", log.BlockNumber,
		)
		processed++
	}
	if processed > 0 {
		slog.Info("processed TaskCompleted", "chain", chainID, "count", processed, "from", from, "to", to)
	}
	return processed, nil
}

// handleTaskDisputed stores the address that raised the dispute.
// Topics[1] taskId, Topics[2] disputedBy
func (h *eventHandler) handleTaskDisputed(
	ctx context.Context,
	client ethereum.LogFilterer,
	chainID int64,
	from, to uint64,
) (int, error) {
	logs, err := h.filterLogs(ctx, client, "TaskDisputed", from, to)
	if err != nil {
		return 0, err
	}

	processed := 0
	for _, log := range logs {
		if len(log.Topics) < 3 {
			continue
		}
		taskID := new(big.Int).SetBytes(log.Topics[1].Bytes()).Int64()
		disputedBy := common.BytesToAddress(log.Topics[2].Bytes()).Hex()

		if err := db.UpdateTaskDisputed(ctx, h.gdb, chainID, taskID, disputedBy); err != nil {
			if errors.Is(err, db.ErrNotFound) {
				slog.Warn("TaskDisputed: task not in DB, skipping",
					"chain", chainID, "id", taskID)
				continue
			}
			return processed, err
		}
		slog.Debug("TaskDisputed",
			"chain", chainID, "id", taskID,
			"disputed_by", shortenAddr(disputedBy), "block", log.BlockNumber,
		)
		processed++
	}
	if processed > 0 {
		slog.Info("processed TaskDisputed", "chain", chainID, "count", processed, "from", from, "to", to)
	}
	return processed, nil
}

// handleDisputeResolved stores the client/executor split from a DisputeResolved event.
// Topics[1] taskId, Topics[2] resolvedBy; Data: clientRefund, executorPayout
func (h *eventHandler) handleDisputeResolved(
	ctx context.Context,
	client ethereum.LogFilterer,
	chainID int64,
	from, to uint64,
) (int, error) {
	logs, err := h.filterLogs(ctx, client, "DisputeResolved", from, to)
	if err != nil {
		return 0, err
	}

	type nonIndexed struct {
		ClientRefund   *big.Int
		ExecutorPayout *big.Int
	}

	processed := 0
	for _, log := range logs {
		if len(log.Topics) < 2 {
			continue
		}
		var d nonIndexed
		if err := h.contractABI.UnpackIntoInterface(&d, "DisputeResolved", log.Data); err != nil {
			return processed, fmt.Errorf("unpack DisputeResolved: %w", err)
		}

		taskID := new(big.Int).SetBytes(log.Topics[1].Bytes()).Int64()
		resolvedBy := common.BytesToAddress(log.Topics[2].Bytes()).Hex()

		if err := db.UpdateDisputeResolved(
			ctx, h.gdb, chainID, taskID,
			d.ClientRefund.String(), d.ExecutorPayout.String(),
		); err != nil {
			if errors.Is(err, db.ErrNotFound) {
				slog.Warn("DisputeResolved: task not in DB, skipping",
					"chain", chainID, "id", taskID)
				continue
			}
			return processed, err
		}
		slog.Debug("DisputeResolved",
			"chain", chainID, "id", taskID,
			"resolved_by", shortenAddr(resolvedBy),
			"client_refund", d.ClientRefund.String(),
			"executor_payout", d.ExecutorPayout.String(),
			"block", log.BlockNumber,
		)
		processed++
	}
	if processed > 0 {
		slog.Info("processed DisputeResolved", "chain", chainID, "count", processed, "from", from, "to", to)
	}
	return processed, nil
}

// handleWithdrawn records each pull-payment withdrawal.
// Topics[1] recipient; Data: amount
func (h *eventHandler) handleWithdrawn(
	ctx context.Context,
	client ethereum.LogFilterer,
	chainID int64,
	from, to uint64,
) (int, error) {
	logs, err := h.filterLogs(ctx, client, "Withdrawn", from, to)
	if err != nil {
		return 0, err
	}

	type nonIndexed struct {
		Amount *big.Int
	}

	processed := 0
	for _, log := range logs {
		if len(log.Topics) < 2 {
			continue
		}
		var d nonIndexed
		if err := h.contractABI.UnpackIntoInterface(&d, "Withdrawn", log.Data); err != nil {
			return processed, fmt.Errorf("unpack Withdrawn: %w", err)
		}

		recipient := common.BytesToAddress(log.Topics[1].Bytes()).Hex()
		txHash := log.TxHash.Hex()

		if err := db.RecordWithdrawal(
			ctx, h.gdb, chainID,
			recipient, d.Amount.String(),
			log.BlockNumber, txHash,
		); err != nil {
			return processed, err
		}
		slog.Debug("Withdrawn",
			"chain", chainID,
			"recipient", shortenAddr(recipient),
			"amount", d.Amount.String(),
			"block", log.BlockNumber,
		)
		processed++
	}
	if processed > 0 {
		slog.Info("processed Withdrawn", "chain", chainID, "count", processed, "from", from, "to", to)
	}
	return processed, nil
}

// handleFeeBpsUpdated logs platform fee configuration changes.
// No indexed params; Data: oldBps, newBps
func (h *eventHandler) handleFeeBpsUpdated(
	ctx context.Context,
	client ethereum.LogFilterer,
	chainID int64,
	from, to uint64,
) (int, error) {
	logs, err := h.filterLogs(ctx, client, "FeeBpsUpdated", from, to)
	if err != nil {
		return 0, err
	}

	type nonIndexed struct {
		OldBps *big.Int
		NewBps *big.Int
	}

	processed := 0
	for _, log := range logs {
		var d nonIndexed
		if err := h.contractABI.UnpackIntoInterface(&d, "FeeBpsUpdated", log.Data); err != nil {
			return processed, fmt.Errorf("unpack FeeBpsUpdated: %w", err)
		}
		slog.Info("FeeBpsUpdated",
			"chain", chainID,
			"old_bps", d.OldBps.Int64(), "new_bps", d.NewBps.Int64(),
			"block", log.BlockNumber,
		)
		processed++
	}
	return processed, nil
}

// handleFeeRecipientUpdated logs fee recipient changes.
// Topics[1] oldRecipient, Topics[2] newRecipient
func (h *eventHandler) handleFeeRecipientUpdated(
	ctx context.Context,
	client ethereum.LogFilterer,
	chainID int64,
	from, to uint64,
) (int, error) {
	logs, err := h.filterLogs(ctx, client, "FeeRecipientUpdated", from, to)
	if err != nil {
		return 0, err
	}

	processed := 0
	for _, log := range logs {
		if len(log.Topics) < 3 {
			continue
		}
		oldRecipient := common.BytesToAddress(log.Topics[1].Bytes()).Hex()
		newRecipient := common.BytesToAddress(log.Topics[2].Bytes()).Hex()
		slog.Info("FeeRecipientUpdated",
			"chain", chainID,
			"old", shortenAddr(oldRecipient), "new", shortenAddr(newRecipient),
			"block", log.BlockNumber,
		)
		processed++
	}
	return processed, nil
}
