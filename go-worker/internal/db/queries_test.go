package db

import (
	"context"
	"testing"
	"time"
)

func TestUpdateFundStateTimestamp_UnknownField_ReturnsNilWithoutDB(t *testing.T) {
	d := &DB{}
	if err := d.UpdateFundStateTimestamp(context.Background(), "", "last_fund_list_sync_at", time.Now()); err != nil {
		t.Fatalf("expected nil error, got %v", err)
	}
}

func TestUpdateFundStateTimestamp_EmptyFundCode_ReturnsError(t *testing.T) {
	d := &DB{}
	if err := d.UpdateFundStateTimestamp(context.Background(), "", "last_details_sync_at", time.Now()); err == nil {
		t.Fatalf("expected error, got nil")
	}
}

