package main

import (
	"context"
	"log"
	"log/slog"
	"os"
	"time"

	"syncer/internal/catalog"
	"syncer/internal/db"
	"syncer/internal/destination"
	"syncer/internal/source"
)

var (
	mongoConnectionString = "mongodb://dba:0sjnrqoLlK7OzE81x3K0doeJY3ii1b7u@localhost:27016/?directConnection=true&authSource=admin"
	mongoDatabaseName     = "warehouseOperationService"
	mongoTableNames       = "sales"
	mongoTableFields      = []string{
		"_id",
		"account.id",
		"account.name",
		"account.username",
		"approved_at",
		"branch.id",
		"branch.name",
		"cash_box.id",
		"cash_box.name",
		"collected_at",
		"contractor.id",
		"contractor.inn",
		"contractor.name",
		"created_at",
		"date",
		"deleted_at",
		"delivery_info",
		"employee.id",
		"employee.name",
		"exact_discounts",
		"exact_discounts.amount",
		"exact_discounts.currency.id",
		"exact_discounts.currency.is_national",
		"exact_discounts.currency.name",
		"handed_over_at",
		"is_approved",
		"is_collected",
		"is_deleted",
		"is_fiscalized",
		"is_for_debt",
		"is_handed_over",
		"is_reviewed",
		"items",
		"items.created_at",
		"items.deleted_at",
		"items.discount.amount",
		"items.discount.type",
		"items.discount.value",
		"items.id",
		"items.is_deleted",
		"items.net_price.amount",
		"items.net_price.currency.id",
		"items.net_price.currency.is_national",
		"items.net_price.currency.name",
		"items.order_item.id",
		"items.order_item.net_price",
		"items.order_item.price.amount",
		"items.order_item.price.currency.id",
		"items.order_item.price.currency.is_national",
		"items.order_item.price.currency.name",
		"items.order_item.quantity",
		"items.original_price.amount",
		"items.original_price.currency.id",
		"items.original_price.currency.is_national",
		"items.original_price.currency.name",
		"items.price.amount",
		"items.price.currency.id",
		"items.price.currency.is_national",
		"items.price.currency.name",
		"items.quantity",
		"items.updated_at",
		"items.warehouse_item.id",
		"items.warehouse_item.marks",
		"items.warehouse_item.name",
		"items.warehouse_item.warehouse.id",
		"items.warehouse_item.warehouse.name",
		"items.warehouse_item.warehouse_item_use.after_quantity",
		"items.warehouse_item.warehouse_item_use.before_quantity",
		"items.warehouse_item.warehouse_item_use.id",
		"location",
		"net_price",
		"net_price.amount",
		"net_price.currency.id",
		"net_price.currency.is_national",
		"net_price.currency.name",
		"note",
		"notes",
		"notes.account.id",
		"notes.account.name",
		"notes.account.username",
		"notes.comment",
		"notes.created_at",
		"notes.deleted_at",
		"notes.id",
		"notes.is_deleted",
		"notes.updated_at",
		"number",
		"order_info.has_sale_shortage",
		"order_info.id",
		"order_info.sale_shortage_items",
		"order_info.type",
		"organization.id",
		"organization.name",
		"payment.cash_box_states",
		"payment.cash_box_states.amount",
		"payment.cash_box_states.currency.id",
		"payment.cash_box_states.currency.is_national",
		"payment.cash_box_states.currency.name",
		"payment.cash_box_states.payment_type",
		"payment.date",
		"payment.debt_states",
		"payment.debt_states.amount",
		"payment.debt_states.currency.id",
		"payment.debt_states.currency.is_national",
		"payment.debt_states.currency.name",
		"payment.id",
		"payment.notes",
		"percent_discount",
		"reviewed_at",
		"status",
		"status_histories",
		"status_histories.date",
		"status_histories.from_status",
		"status_histories.id",
		"status_histories.to_status",
		"tags",
		"updated_at",
		"used_warehouses",
	}

	postgresConnectionString = "postgresql://postgres:postgres@localhost:5432/report_service"
	primaryKey               = "_id"
)

func main() {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()

	// ── 1. Connect to MongoDB ────────────────────────────────────────────────
	mongoCli, err := db.ConnectMongo(db.MongoConfig{
		URI: mongoConnectionString,
	})
	if err != nil {
		log.Fatal(err)
	}
	defer db.DisconnectMongo(mongoCli)

	// ── 2. Discover schema ───────────────────────────────────────────────────
	mongoCat, err := catalog.NewMongoDiscoverer(mongoCli, mongoDatabaseName).Discover(ctx)
	if err != nil {
		log.Fatal(err)
	}

	log.Println("\n=== MongoDB catalog ===")
	catalog.PrintCatalog(os.Stdout, mongoCat)

	// ── 3. Find the target stream and filter to requested fields ─────────────
	discovered, ok := mongoCat.Get(mongoDatabaseName, mongoTableNames)
	if !ok {
		log.Fatalf("collection %q not found in catalog", mongoTableNames)
	}
	stream := discovered.FilterFields(mongoTableFields)

	// ── 4. Connect to Postgres ───────────────────────────────────────────────
	pool, err := destination.NewPool(ctx, postgresConnectionString)
	if err != nil {
		log.Fatal(err)
	}

	// ── 5. Ensure destination table exists ───────────────────────────────────
	if err := destination.EnsureTable(ctx, pool, "", mongoTableNames, stream); err != nil {
		log.Fatal(err)
	}

	// ── 6. Read from MongoDB, write to Postgres ──────────────────────────────
	writer := destination.NewWriter(pool, destination.WriterConfig{
		Schema:     "",
		Table:      mongoTableNames,
		Mode:       destination.WriteModeOverwrite,
		PrimaryKey: []string{primaryKey},
	}, slog.Default())

	ch, err := source.ReadCollection(ctx, mongoCli, mongoDatabaseName, mongoTableNames, stream)
	if err != nil {
		log.Fatal(err)
	}

	result, err := writer.Write(ctx, stream, ch)
	if err != nil {
		log.Fatal(err)
	}

	log.Printf("done: %d rows in %d batches (%s)", result.RowsCopied, result.Batches, result.Duration)
}
